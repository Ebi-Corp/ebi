use crate::redb::*;
use crate::service::state::{GroupState, StateDatabase};
use crate::{Shelf, Workspace};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::sharedref::*;
use ebi_types::shelf::*;
use ebi_types::tag::TagId;
use ebi_types::workspace::{WorkspaceId, WorkspaceInfo};
use ebi_types::{NodeId, Uuid, tag::Tag};
use redb::ReadableTable;
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::Service;

#[derive(Clone)]
pub struct ScopedDatabase {
    pub(crate) service: StateDatabase,
    pub(crate) workspace_scope: WorkspaceId,
}

impl ScopedDatabase {
    pub async fn create_tag(
        &mut self,
        priority: u64,
        name: String,
        parent: Option<Uuid>,
    ) -> Result<TagId, ReturnCode> {
        self.call(CreateTag {
            priority,
            name,
            parent,
        })
        .await
    }

    pub async fn delete_tag(&mut self, tag_id: Uuid) -> Result<TagRef, ReturnCode> {
        self.call(DeleteTag { tag_id }).await
    }

    pub async fn unassign_shelf(&mut self, shelf_id: ShelfId) -> Result<(), ReturnCode> {
        self.call(UnassignShelf { shelf_id }).await
    }

    pub async fn assign_shelf(
        &mut self,
        path: PathBuf,
        node_id: NodeId,
        remote: bool,
        name: Option<String>,
        description: Option<String>,
    ) -> Result<ShelfId, ReturnCode> {
        self.call(AssignShelf {
            path,
            node_id,
            remote,
            name,
            description,
        })
        .await
    }

    pub async fn edit_shelf_info(
        &mut self,
        shelf_id: ShelfId,
        name: String,
        description: String,
    ) -> Result<(), ReturnCode> {
        self.call(EditShelf {
            shelf_id,
            name,
            description,
        })
        .await
    }

    pub async fn edit_workspace_info(
        &mut self,
        name: String,
        description: String,
    ) -> Result<(), ReturnCode> {
        self.call(EditWorkspace { name, description }).await
    }
}

impl ScopedDatabase {
    fn set_workspace(&self) -> Result<Arc<StatefulRef<Workspace>>, ReturnCode> {
        self.service
            .state
            .load()
            .staged
            .load()
            .workspaces
            .get(&self.workspace_scope)
            .cloned()
            .ok_or(ReturnCode::WorkspaceNotFound)
    }
}

struct CreateTag {
    pub priority: u64,
    pub name: String,
    pub parent: Option<Uuid>,
}

impl Service<CreateTag> for ScopedDatabase {
    type Response = TagId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;
        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let workspace = workspace_ref.load_full();

            let parent = match req.parent {
                Some(p_id) => Some(
                    workspace
                        .tags
                        .get(&p_id)
                        .ok_or(ReturnCode::ParentNotFound)?
                        .clone(),
                ),
                None => None,
            };
            let tag = Tag {
                priority: req.priority,
                name: req.name.clone(),
                parent,
            };
            let tag_ref = SharedRef::<Tag>::new_ref(tag);
            workspace_ref
                .stateful_rcu(|w| {
                    let (u_l, _) = w.lookup.insert(req.name.clone(), tag_ref.id);
                    let (u_t, u_s) = w.tags.insert(tag_ref.id, tag_ref.clone());
                    let u_w = Workspace {
                        tags: u_t,
                        info: w.info.clone_inner(),
                        shelves: w.shelves.clone(),
                        lookup: u_l,
                    };
                    (u_w, u_s)
                })
                .await;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut tag_t = write_txn
                    .open_table(T_TAG)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                tag_t
                    .insert(tag_ref.id, tag_ref.to_storable())
                    .map_err(|_| ReturnCode::DbCommitError)?;
                wk.0.tags.push(tag_ref.id);
                wk.0.lookup.insert(req.name.clone(), tag_ref.id);
                if *modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                    entity_state_t.insert(staged_id, state_hmap).unwrap();
                }
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(tag_ref.id)
        })
    }
}

struct DeleteTag {
    tag_id: Uuid,
}

impl Service<DeleteTag> for ScopedDatabase {
    type Response = TagRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;
        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let workspace = workspace_ref.load();

            let Some(tag_ref) = workspace.tags.get(&req.tag_id) else {
                return Err(ReturnCode::TagNotFound);
            };

            workspace_ref
                .stateful_rcu(|w| {
                    let (u_m, u_s) = w.tags.remove(&req.tag_id);
                    let u_w = Workspace {
                        tags: u_m,
                        info: w.info.clone_inner(),
                        shelves: w.shelves.clone(),
                        lookup: w.lookup.clone(),
                    };
                    (u_w, u_s)
                })
                .await;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut tag_t = write_txn
                    .open_table(T_TAG)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                wk.0.tags.retain(|id| *id != tag_ref.id);
                tag_t
                    .remove(tag_ref.id)
                    .map_err(|_| ReturnCode::DbCommitError)?;

                if *modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                    entity_state_t.insert(staged_id, state_hmap).unwrap();
                }
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(tag_ref.clone())
        })
    }
}

struct UnassignShelf {
    pub shelf_id: ShelfId,
}

impl Service<UnassignShelf> for ScopedDatabase {
    type Response = (); // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnassignShelf) -> Self::Future {
        let g_state = self.service.state.load().clone();
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;
        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let state = g_state.staged.load();
            let Some(_) = state.shelves.get(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };

            workspace_ref
                .stateful_rcu(|w| {
                    let (u_m, u_s) = w.shelves.remove(&req.shelf_id);
                    let u_w = Workspace {
                        info: w.info.clone_inner(),
                        shelves: u_m,
                        tags: w.tags.clone(),
                        lookup: w.lookup.clone(),
                    };
                    (u_w, u_s)
                })
                .await;

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut shelf_t = write_txn
                    .open_table(T_SHELF)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)
                    .cloned()?;
                let (sk_db_id, _) = state_hmap
                    .0
                    .get(&req.shelf_id)
                    .ok_or(ReturnCode::InternalStateError)?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                wk.0.shelves.retain(|id| *id != req.shelf_id);
                shelf_t.remove(sk_db_id).unwrap();
                state_hmap.0.remove(&req.shelf_id);

                if modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                }
                entity_state_t.insert(staged_id, state_hmap).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(())
        })
    }
}

struct AssignShelf {
    pub path: PathBuf,
    pub node_id: NodeId,
    pub remote: bool,
    pub description: Option<String>,
    pub name: Option<String>,
}

impl Service<AssignShelf> for ScopedDatabase {
    type Response = ShelfId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AssignShelf) -> Self::Future {
        let g_state = self.service.state.load().clone();
        let _lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;

        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let state = g_state.staged.load();

            // The ID is deterministically created on node + path
            let bytes = [
                &req.node_id.as_bytes()[0..8],
                req.path.to_str().unwrap().as_bytes(),
            ]
            .concat();
            let shelf_id = Uuid::new_v5(&Uuid::NAMESPACE_DNS, &bytes);

            let mut new_shelf: bool = true;
            let mut shelf_ref: Option<ImmutRef<Shelf>> = None;

            match state.shelves.get(&shelf_id) {
                Some(shelf_weak_ref) => {
                    if let Some(shelf_up_ref) = shelf_weak_ref.to_upgraded() {
                        new_shelf = false;
                        shelf_ref = Some(shelf_up_ref)
                    }
                }
                None => {
                    g_state
                        .staged
                        .stateful_rcu(|w| {
                            let (u_m, u_s) = w.shelves.remove(&shelf_id);
                            let u_w = GroupState {
                                shelves: u_m,
                                workspaces: w.workspaces.clone(),
                            };
                            (u_w, u_s)
                        })
                        .await;
                }
            }

            if new_shelf {
                let path = req.path.clone();
                let shelf_type = if !req.remote {
                    ShelfType::Local
                } else {
                    ShelfType::Remote
                };
                let shelf = Shelf::new(
                    path,
                    req.name.unwrap_or_else(|| {
                        req.path
                            .clone()
                            .components()
                            .next_back()
                            .and_then(|comp| comp.as_os_str().to_str())
                            .unwrap_or_default()
                            .to_string()
                    }),
                    shelf_type,
                    ShelfOwner::Node(req.node_id),
                    None,
                    req.description.unwrap_or_default(),
                );

                shelf_ref = Some(ImmutRef::new_ref_id(shelf_id, shelf))
            }

            let shelf_ref = shelf_ref.unwrap();

            let shelf_id = shelf_ref.id;

            workspace_ref
                .stateful_rcu(|w| {
                    let (u_m, u_s) = w.shelves.insert(shelf_id, shelf_ref.clone());
                    let u_w = Workspace {
                        info: w.info.clone_inner(),
                        shelves: u_m,
                        tags: w.tags.clone(),
                        lookup: w.lookup.clone(),
                    };
                    (u_w, u_s)
                })
                .await;

            if new_shelf {
                g_state
                    .staged
                    .stateful_rcu(|s| {
                        let (u_m, u_s) = s.shelves.insert(shelf_id, shelf_ref.downgrade());
                        let u_g = GroupState {
                            shelves: u_m,
                            workspaces: s.workspaces.clone(),
                        };
                        (u_g, u_s)
                    })
                    .await;
            }

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut shelf_t = write_txn
                    .open_table(T_SHELF)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)
                    .cloned()?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                if new_shelf {
                    let shelf_db_id = Uuid::new_v4();
                    state_hmap.0.insert(shelf_id, (shelf_db_id, true));
                    shelf_t
                        .insert(shelf_db_id, shelf_ref.to_storable())
                        .unwrap();
                }
                wk.0.shelves.push(shelf_id);
                if modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                }

                entity_state_t.insert(staged_id, state_hmap).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(shelf_id)
        })
    }
}

struct EditShelf {
    pub shelf_id: ShelfId,
    pub name: String,
    pub description: String,
}

impl Service<EditShelf> for ScopedDatabase {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditShelf) -> Self::Future {
        let _lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;

        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let workspace = workspace_ref.load();
            let Some(shelf_ref) = workspace.shelves.get(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };
            match shelf_ref.shelf_type {
                ShelfType::Local => {
                    let s = (***shelf_ref).clone();
                    s.info
                        .stateful_rcu(|info| {
                            let (u_f, u_s) = info.name.set(&req.name);
                            let u_i = ShelfInfo {
                                name: u_f,
                                description: info.description.clone(),
                                root: info.root.clone(),
                            };
                            (u_i, u_s)
                        })
                        .await;
                    s.info
                        .stateful_rcu(|info| {
                            let (u_f, u_s) = info.description.set(&req.description);
                            let u_i = ShelfInfo {
                                name: info.name.clone(),
                                description: u_f,
                                root: info.root.clone(),
                            };
                            (u_i, u_s)
                        })
                        .await;
                    workspace_ref
                        .stateful_rcu(|w| {
                            let (u_m, u_s) =
                                w.shelves.insert(shelf_ref.id, ImmutRef::new_ref(s.clone()));
                            let u_w = Workspace {
                                shelves: u_m,
                                tags: w.tags.clone(),
                                lookup: w.lookup.clone(),
                                info: w.info.clone_inner(),
                            };
                            (u_w, u_s)
                        })
                        .await;
                    if let ShelfOwner::Sync(_sync_id) = shelf_ref.shelf_owner {
                        todo!();
                    }
                }
                ShelfType::Remote => match shelf_ref.shelf_owner {
                    ShelfOwner::Node(_peer_id) => {
                        todo!();
                    }
                    ShelfOwner::Sync(_sync_id) => {
                        todo!();
                    }
                },
            }

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut shelf_t = write_txn
                    .open_table(T_SHELF)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, w_modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)
                    .cloned()?;
                let (sk_db_id, s_modified) = state_hmap
                    .0
                    .get(&shelf_ref.id)
                    .ok_or(ReturnCode::InternalStateError)
                    .cloned()?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                if s_modified {
                    shelf_t.insert(sk_db_id, shelf_ref.to_storable()).unwrap();
                } else {
                    let new_shelf_db_id = Uuid::new_v4();
                    shelf_t
                        .insert(new_shelf_db_id, shelf_ref.to_storable())
                        .unwrap();
                    state_hmap.0.insert(shelf_ref.id, (new_shelf_db_id, true));
                }

                wk.0.shelves.push(shelf_ref.id);
                if w_modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                }
                entity_state_t.insert(staged_id, state_hmap).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;
            Ok(())
        })
    }
}

struct EditWorkspace {
    pub name: String,
    pub description: String,
}

impl Service<EditWorkspace> for ScopedDatabase {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditWorkspace) -> Self::Future {
        let _lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        let staged_id = self.service.state.load().staged.id;

        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let workspace = workspace_ref.load();
            workspace
                .info
                .stateful_rcu(|info| {
                    let (u_f, u_s) = info.name.set(&req.name);
                    let u_i = WorkspaceInfo {
                        name: u_f,
                        description: info.description.clone(),
                    };
                    (u_i, u_s)
                })
                .await;
            workspace
                .info
                .stateful_rcu(|info| {
                    let (u_f, u_s) = info.description.set(&req.description);
                    let u_i = WorkspaceInfo {
                        name: info.name.clone(),
                        description: u_f,
                    };
                    (u_i, u_s)
                })
                .await;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (wk_db_id, w_modified) = state_hmap
                    .0
                    .get(&workspace_ref.id)
                    .ok_or(ReturnCode::InternalStateError)?;
                let mut wk = wk_t
                    .get(wk_db_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();
                let info = workspace_ref.load().info.load();
                wk.0.name = info.name.get();
                wk.0.description = info.description.get();

                if *w_modified {
                    wk_t.insert(wk_db_id, wk).unwrap();
                } else {
                    let new_wk_db = Uuid::new_v4();
                    wk_t.insert(new_wk_db, wk).unwrap();
                    state_hmap.0.insert(workspace_ref.id, (new_wk_db, true));
                    entity_state_t.insert(staged_id, state_hmap).unwrap();
                }
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ebi_types::NodeId;
    use ebi_types::shelf::{ShelfOwner, ShelfType};
    use std::str::FromStr;

    const TEST_PKEY: &str = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";

    async fn setup_state_db(test_name: &str) -> (StateDatabase, Uuid) {
        let test_path = std::env::temp_dir().join("ebi-database");
        let test_path = test_path.join(test_name);
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let _ = std::fs::remove_file(&db_path);
        let mut state_db = StateDatabase::new(&db_path).unwrap();

        let wk_name = "workspace".to_string();
        let wk_desc = "none".to_string();

        let wk_id = state_db.create_workspace(wk_name, wk_desc).await.unwrap();
        (state_db, wk_id)
    }

    #[tokio::test]
    async fn create_tag() {
        let (mut state_db, wk_id) = setup_state_db("create-tag").await;

        let t_priority: u64 = 16;
        let t_name = "tag_name".to_string();

        let t_id = state_db
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), None)
            .await
            .unwrap();

        let wk = state_db
            .state
            .load()
            .staged
            .load()
            .workspaces
            .get(&wk_id)
            .unwrap()
            .load();

        let tag = wk.tags.get(&t_id);

        assert!(tag.is_some());

        let tag = tag.unwrap().load();
        assert_eq!(tag.name.as_str(), t_name.as_str());
        assert_eq!(tag.priority, t_priority);
        assert_eq!(tag.parent, None);
        let prev_tag = tag;

        let parent = Some(t_id);
        let t_priority = 10;
        let t_id = state_db
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), parent)
            .await
            .unwrap();

        let wk = state_db
            .state
            .load()
            .staged
            .load()
            .workspaces
            .get(&wk_id)
            .unwrap()
            .load();

        let tag = wk.tags.get(&t_id);
        assert!(tag.is_some());
        let tag = tag.unwrap().load();
        assert_eq!(tag.name.as_str(), t_name.as_str());
        assert_eq!(tag.priority, t_priority);
        assert!(tag.parent.is_some());
        let tag_p_id = tag.parent.as_ref().unwrap().id;
        assert_eq!(Some(tag_p_id), parent);
        assert_eq!(
            tag.parent.as_ref().unwrap().load().as_ref(),
            prev_tag.as_ref()
        );
    }

    #[tokio::test]
    async fn delete_tag() {
        let (mut state_db, wk_id) = setup_state_db("delete-tag").await;

        let t_priority: u64 = 16;
        let t_name = "tag_name".to_string();

        let t_id = state_db
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), None)
            .await
            .unwrap();

        let _ = state_db.workspace(wk_id).delete_tag(t_id).await.unwrap();

        let wk = state_db
            .state
            .load()
            .staged
            .load()
            .workspaces
            .get(&wk_id)
            .unwrap()
            .load();

        let tag = wk.tags.get(&t_id);

        assert!(tag.is_none());

        // [TODO] handle parents
    }

    #[tokio::test]
    async fn assign_shelf() {
        let (mut state_db, wk_id) = setup_state_db("assign-shelf").await;
        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let test_path = std::env::temp_dir().join("ebi-database");

        let shelf_name = "shelf_name".to_string();
        let shelf_description = "shelf_description".to_string();
        let remote = false;

        let s_0_id = state_db
            .workspace(wk_id)
            .assign_shelf(
                test_path.clone(),
                node_id,
                remote,
                Some(shelf_name.clone()),
                Some(shelf_description.clone()),
            )
            .await
            .unwrap();

        let state = state_db.state.load().staged.load();
        let wk = state.workspaces.get(&wk_id).unwrap().load();
        let s = wk.shelves.get(&s_0_id);

        assert!(s.is_some());
        let s = s.unwrap();
        assert_eq!(s.info.load().name.get(), shelf_name);
        assert_eq!(s.info.load().description.get(), shelf_description);
        assert_eq!(s.shelf_type, ShelfType::Local);
        assert_eq!(s.shelf_owner, ShelfOwner::Node(node_id));

        let s_ref = state.shelves.get(&s_0_id).unwrap();

        let s_ref = s_ref.to_upgraded().unwrap();
        assert!(ptr_eq(s_ref.data_ref(), s.data_ref()));
        assert_eq!(s_ref.id, s.id);
    }

    #[tokio::test]
    async fn unassign_shelf() {
        let (mut state_db, wk_id) = setup_state_db("unassign-shelf").await;
        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let test_path = std::env::temp_dir().join("ebi-database");

        let shelf_name = "shelf_name".to_string();
        let shelf_description = "shelf_description".to_string();
        let remote = false;

        let s_0_id = state_db
            .workspace(wk_id)
            .assign_shelf(
                test_path.clone(),
                node_id,
                remote,
                Some(shelf_name.clone()),
                Some(shelf_description.clone()),
            )
            .await
            .unwrap();

        let _ = state_db
            .workspace(wk_id)
            .unassign_shelf(s_0_id)
            .await
            .unwrap();

        let state = state_db.state.load().staged.load();
        let wk = state.workspaces.get(&wk_id).unwrap().load();
        let s = wk.shelves.get(&s_0_id);
        assert!(s.is_none());
        let s_ref = state.shelves.get(&s_0_id).unwrap();
        assert!(s_ref.to_upgraded().is_none());
    }

    #[tokio::test]
    async fn edit_workspace_info() {
        todo!();
    }

    #[tokio::test]
    async fn edit_shelf_info() {
        todo!();
    }
}
