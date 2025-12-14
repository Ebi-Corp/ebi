use crate::service::state::{GroupState, StateDatabase};
use crate::{Shelf, Workspace};
use ebi_proto::rpc::ReturnCode;
use ebi_types::sharedref::*;
use ebi_types::shelf::*;
use ebi_types::tag::TagId;
use ebi_types::workspace::{WorkspaceId, WorkspaceInfo};
use ebi_types::{NodeId, Uuid, tag::Tag};
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
        description: Option<String>,
        name: Option<String>,
    ) -> Result<ShelfId, ReturnCode> {
        self.call(AssignShelf {
            path,
            node_id,
            remote,
            description,
            name,
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
                parent
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
        let g_state = self.service.state.clone();
        let res_workspace_ref = self.set_workspace();
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
        let g_state = self.service.state.clone();
        let lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();

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
                let Ok(shelf) = Shelf::new(
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
                ) else {
                    return Err(ReturnCode::ShelfCreationIOError);
                };

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
        let lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();

        Box::pin(async move {
            let workspace_ref = res_workspace_ref?;
            let workspace = workspace_ref.load();
            let Some(shelf) = workspace.shelves.get(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };
            match shelf.shelf_type {
                ShelfType::Local => {
                    let s = (***shelf).clone();
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
                                w.shelves.insert(shelf.id, ImmutRef::new_ref(s.clone()));
                            let u_w = Workspace {
                                shelves: u_m,
                                tags: w.tags.clone(),
                                lookup: w.lookup.clone(),
                                info: w.info.clone_inner(),
                            };
                            (u_w, u_s)
                        })
                        .await;
                    if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                        todo!();
                    }
                }
                ShelfType::Remote => match shelf.shelf_owner {
                    ShelfOwner::Node(peer_id) => {
                        todo!();
                    }
                    ShelfOwner::Sync(_sync_id) => {
                        todo!();
                    }
                },
            }
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
        let lock = self.service.lock.clone();
        let res_workspace_ref = self.set_workspace();

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
            Ok(())
        })
    }
}
