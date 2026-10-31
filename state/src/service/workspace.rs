use crate::service::state::StateService;
use crate::{Shelf, Workspace};
use chrono::Utc;

use crate::{SyncState, redb::*};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::sharedref::*;
use ebi_types::shelf::*;
use ebi_types::tag::TagId;
use ebi_types::workspace::WorkspaceId;
use ebi_types::{NodeId, Uuid, tag::Tag};
use redb::ReadableTable;
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;

#[derive(Clone)]
pub struct WorkspaceStateService {
    pub(crate) service: StateService,
    pub(crate) scope: WorkspaceId,
}

impl WorkspaceStateService {
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

impl WorkspaceStateService {
    fn set_workspace(&self) -> Result<SharedRef<SyncState<Workspace>>, ReturnCode> {
        self.service
            .sync_states
            .pin()
            .get(&self.scope)
            .cloned()
            .ok_or(ReturnCode::WorkspaceNotFound)
    }
}

struct CreateTag {
    pub priority: u64,
    pub name: String,
    pub parent: Option<Uuid>,
}

impl Service<CreateTag> for WorkspaceStateService {
    type Response = TagId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        Box::pin(async move {
            if req.name.is_empty() {
                return Err(ReturnCode::TagNameEmpty);
            }

            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let workspace = workspace_ref.load();

            if workspace.lookup.contains_key(&req.name) {
                return Err(ReturnCode::TagNameDuplicate);
            }

            let parent = match req.parent {
                Some(p_id) => {
                    if !workspace.tags.contains_key(&p_id) {
                        return Err(ReturnCode::ParentNotFound);
                    }
                    Some(p_id)
                }
                None => None,
            };
            let tag = Tag::created(req.name.clone(), req.priority, parent, Utc::now());
            // should tag id be generated from workspace name/id + tag name ?
            let tag_ref = SharedRef::<Tag>::new_ref(Uuid::new_v4(), tag);
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
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_workspaces_t = write_txn
                    .open_table(T_ENTITY_WORKSPACES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_t = write_txn
                    .open_table(T_ENTITY)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut entities = workspace_entities_t
                    .get(staged_id)
                    .unwrap()
                    .unwrap()
                    .value();

                let mut wk = workspace_t
                    .get(staged_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                tag_t
                    .insert(tag_ref.id, tag_ref.to_storable())
                    .map_err(|_| ReturnCode::DbCommitError)?;
                wk.0.tags.push(tag_ref.id);
                wk.0.lookup.insert(req.name.clone(), tag_ref.id);
                let t_entity_id = Uuid::new_v4();
                workspace_t
                    .insert(staged_id, wk)
                    .map_err(|_| ReturnCode::DbCommitError)?;
                let tag_e = Entity {
                    id: tag_ref.id,
                    kind: EntityKind::Tag,
                };
                entities.push(tag_e.to_storable());
                workspace_entities_t.insert(staged_id, entities).unwrap();
                entity_workspaces_t
                    .insert(t_entity_id, vec![staged_id])
                    .map_err(|_| ReturnCode::DbCommitError)?;
                entity_t
                    .insert(tag_ref.id, vec![tag_e.to_storable()])
                    .map_err(|_| ReturnCode::DbCommitError)?;
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(tag_ref.id)
        })
    }
}

struct DeleteTag {
    tag_id: Uuid,
}

impl Service<DeleteTag> for WorkspaceStateService {
    type Response = TagRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();
        Box::pin(async move {
            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let workspace = workspace_ref.load();

            let Some(tag_ref) = workspace.tags.get(&req.tag_id) else {
                return Err(ReturnCode::TagNotFound);
            };

            let tag_name = tag_ref.load().name.get().clone();

            workspace_ref
                .stateful_rcu(|w| {
                    let (u_m, _) = w.tags.remove(&req.tag_id);
                    let (u_l, u_s) = w.lookup.remove(&tag_name);
                    let u_w = Workspace {
                        tags: u_m,
                        info: w.info.clone_inner(),
                        shelves: w.shelves.clone(),
                        lookup: u_l,
                    };
                    (u_w, u_s)
                })
                .await;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_workspaces_t = write_txn
                    .open_table(T_ENTITY_WORKSPACES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_t = write_txn
                    .open_table(T_ENTITY)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut tag_t = write_txn
                    .open_table(T_TAG)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut wk = workspace_t.get(staged_id).unwrap().unwrap().value();

                wk.0.tags.retain(|id| *id != tag_ref.id);
                wk.0.lookup.remove(&tag_name);

                workspace_t.insert(staged_id, wk).unwrap();
                let mut entities = workspace_entities_t
                    .get(staged_id)
                    .unwrap()
                    .unwrap()
                    .value();

                let mut tags = entity_t.get(tag_ref.id).unwrap().unwrap().value();
                let mut to_remove = Vec::new();
                // we only remove the tag entity associated to current workspace state
                for tag_e in tags.iter() {
                    if entities.contains(&tag_e) {
                        to_remove.push(tag_e.clone());
                    }
                }

                // to_remove size should be 1, maybe use Option ?
                for tag_e in to_remove {
                    tags.retain(|tag| *tag != tag_e); // remove from T_ENTITY
                    entities.retain(|id| *id != tag_e); // remove from T_WORKSPACE_ENTITIES
                    tag_t
                        .remove(tag_e.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                    entity_workspaces_t
                        .remove(tag_e.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }

                // if we removed the only entity id associated to tag
                if tags.is_empty() {
                    entity_t
                        .remove(tag_ref.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }

                workspace_entities_t
                    .insert(staged_id, entities)
                    .map_err(|_| ReturnCode::DbCommitError)?;
                // note: T_TAG from previous workspace states must be removed separately
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(tag_ref.clone())
        })
    }
}

struct UnassignShelf {
    pub shelf_id: ShelfId,
}

impl Service<UnassignShelf> for WorkspaceStateService {
    type Response = (); // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnassignShelf) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let shelves = self.service.shelves.clone();
        let db = self.service.db.clone();

        Box::pin(async move {
            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let Some(_) = shelves.pin().get(&req.shelf_id) else {
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
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_workspaces_t = write_txn
                    .open_table(T_ENTITY_WORKSPACES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_t = write_txn
                    .open_table(T_ENTITY)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut shelf_t = write_txn
                    .open_table(T_SHELF)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut wk = workspace_t
                    .get(staged_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                wk.0.shelves.retain(|id| *id != req.shelf_id);

                let mut entities = workspace_entities_t
                    .get(staged_id)
                    .unwrap()
                    .unwrap()
                    .value();
                let mut shelves = entity_t.get(&req.shelf_id).unwrap().unwrap().value();

                // to_remove size should be 1, maybe use Option ?
                let mut to_remove = Vec::new();

                // we only remove the shelf entity associated to current workspace state
                for shelf_e in &shelves {
                    if entities.contains(&shelf_e) {
                        to_remove.push(shelf_e.clone());
                    }
                }

                for shelf_e in to_remove {
                    shelves.retain(|id| *id != shelf_e); // remove from T_ENTITY
                    entities.retain(|id| *id != shelf_e); // remove from T_WORKSPACE_ENTITIES
                    shelf_t
                        .remove(shelf_e.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                    entity_workspaces_t
                        .remove(shelf_e.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }

                workspace_t
                    .insert(staged_id, wk)
                    .map_err(|_| ReturnCode::DbCommitError)?;
                // if we removed the only entity id associated to tag
                if shelves.is_empty() {
                    entity_t
                        .remove(&req.shelf_id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }

                workspace_entities_t
                    .insert(staged_id, entities)
                    .map_err(|_| ReturnCode::DbCommitError)?;
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

impl Service<AssignShelf> for WorkspaceStateService {
    type Response = ShelfId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AssignShelf) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let shelves = self.service.shelves.clone();
        let db = self.service.db.clone();

        Box::pin(async move {
            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let shelves = shelves.pin_owned();

            // The ID is deterministically created on node + path
            let bytes = [
                &req.node_id.as_bytes()[0..8],
                req.path.to_str().unwrap().as_bytes(),
            ]
            .concat();
            let shelf_id = Uuid::new_v5(&Uuid::NAMESPACE_DNS, &bytes);

            let mut new_shelf: bool = true;
            let mut shelf_ref: Option<ImmutRef<Shelf>> = None;

            match shelves.get(&shelf_id) {
                Some(shelf_weak_ref) => {
                    if let Some(shelf_up_ref) = ImmutRef::<Shelf>::upgraded(shelf_weak_ref) {
                        new_shelf = false;
                        shelf_ref = Some(shelf_up_ref)
                    }
                }
                None => {
                    shelves.remove(&shelf_id);
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

                shelf_ref = Some(ImmutRef::new_ref(shelf_id, shelf))
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
                shelves.insert(shelf_ref.downgraded());
            }

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut shelf_t = write_txn
                    .open_table(T_SHELF)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_workspaces_t = write_txn
                    .open_table(T_ENTITY_WORKSPACES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_t = write_txn
                    .open_table(T_ENTITY)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut entities = workspace_entities_t
                    .get(staged_id)
                    .unwrap()
                    .unwrap()
                    .value();

                let mut wk = workspace_t
                    .get(staged_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();

                let s_entity = Entity {
                    id: Uuid::new_v4(),
                    kind: EntityKind::Shelf,
                };
                if new_shelf {
                    shelf_t
                        .insert(s_entity.id, shelf_ref.to_storable())
                        .map_err(|_| ReturnCode::DbCommitError)?;
                    entity_t
                        .insert(shelf_id, vec![s_entity.to_storable()])
                        .map_err(|_| ReturnCode::DbCommitError)?;
                } else {
                    let mut shelf_entities = entity_t
                        .get(shelf_id)
                        .unwrap()
                        .ok_or(ReturnCode::InternalStateError)?
                        .value();
                    shelf_entities.push(s_entity.to_storable());
                    entity_t
                        .insert(shelf_id, shelf_entities)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }

                entities.push(s_entity.to_storable());

                entity_workspaces_t
                    .insert(s_entity.id, vec![staged_id])
                    .map_err(|_| ReturnCode::DbCommitError)?;
                wk.0.shelves.push(shelf_id);
                workspace_t
                    .insert(staged_id, wk)
                    .map_err(|_| ReturnCode::DbCommitError)?;

                workspace_entities_t
                    .insert(staged_id, entities)
                    .map_err(|_| ReturnCode::DbCommitError)?;
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

impl Service<EditShelf> for WorkspaceStateService {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditShelf) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();

        Box::pin(async move {
            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let workspace = workspace_ref.load();

            let Some(shelf_ref) = workspace.shelves.get(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };
            let edited: Shelf;

            match shelf_ref.shelf_type {
                ShelfType::Local => {
                    let s = (***shelf_ref).clone();
                    let edited_at = Utc::now();
                    s.info.rcu(|info| {
                        info.edited(req.name.clone(), req.description.clone(), edited_at)
                    });
                    workspace_ref
                        .stateful_rcu(|w| {
                            let (u_m, u_s) = w
                                .shelves
                                .insert(shelf_ref.id, ImmutRef::new_ref(shelf_ref.id, s.clone()));
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
                    edited = s;
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
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_workspaces_t = write_txn
                    .open_table(T_ENTITY_WORKSPACES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_t = write_txn
                    .open_table(T_ENTITY)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut entities = workspace_entities_t
                    .get(staged_id)
                    .unwrap()
                    .unwrap()
                    .value();

                let s_entity = Entity {
                    id: Uuid::new_v4(),
                    kind: EntityKind::Shelf,
                };

                let mut shelf_entities = entity_t
                    .get(&req.shelf_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();
                shelf_entities.push(s_entity.to_storable());
                entity_t
                    .insert(&req.shelf_id, shelf_entities)
                    .map_err(|_| ReturnCode::DbCommitError)?;
                let edited_ref: ImmutRef<Shelf> = ImmutRef::new_ref(shelf_ref.id, edited);
                shelf_t
                    .insert(s_entity.id, edited_ref.to_storable())
                    .map_err(|_| ReturnCode::DbCommitError)?;

                entities.push(s_entity.to_storable());

                entity_workspaces_t
                    .insert(s_entity.id, vec![staged_id])
                    .map_err(|_| ReturnCode::DbCommitError)?;

                workspace_entities_t
                    .insert(staged_id, entities)
                    .map_err(|_| ReturnCode::DbCommitError)?;
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

impl Service<EditWorkspace> for WorkspaceStateService {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditWorkspace) -> Self::Future {
        let res_workspace_ref = self.set_workspace();
        let db = self.service.db.clone();

        Box::pin(async move {
            let workspace_ref = &res_workspace_ref?.load_full().staged;
            let staged_id = workspace_ref.id;
            let workspace = workspace_ref.load();

            let edited_at = Utc::now();
            workspace
                .info
                .rcu(|info| info.edited(req.name.clone(), req.description.clone(), edited_at));
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                let mut wk = workspace_t
                    .get(staged_id)
                    .unwrap()
                    .ok_or(ReturnCode::InternalStateError)?
                    .value();
                let info = workspace_ref.load().info.load();
                wk.0.name = info.name.clone();
                wk.0.description = info.description.clone();

                workspace_t
                    .insert(staged_id, wk)
                    .map_err(|_| ReturnCode::DbCommitError)?;
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

    async fn setup_state_service(test_name: &str) -> (StateService, Uuid) {
        let test_path = std::env::temp_dir().join("ebi-state");
        let test_path = test_path.join(test_name);
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let _ = std::fs::remove_file(&db_path);
        let mut state_service = StateService::new(&db_path).unwrap();

        let wk_name = "workspace".to_string();
        let wk_desc = "none".to_string();

        let wk_id = state_service
            .create_workspace(wk_name, wk_desc)
            .await
            .unwrap();
        (state_service, wk_id)
    }

    #[tokio::test]
    async fn create_tag_rejects_empty_name() {
        let (mut state_service, wk_id) = setup_state_service("create-tag-empty").await;

        assert_eq!(
            state_service
                .workspace(wk_id)
                .create_tag(1, String::new(), None)
                .await,
            Err(ReturnCode::TagNameEmpty)
        );
    }

    #[tokio::test]
    async fn create_tag_rejects_duplicate_name() {
        let (mut state_service, wk_id) = setup_state_service("create-tag-duplicate").await;

        let t_id = state_service
            .workspace(wk_id)
            .create_tag(1, "dup".to_string(), None)
            .await
            .unwrap();

        assert_eq!(
            state_service
                .workspace(wk_id)
                .create_tag(2, "dup".to_string(), None)
                .await,
            Err(ReturnCode::TagNameDuplicate)
        );

        let wk = state_service
            .sync_states
            .pin()
            .get(&wk_id)
            .unwrap()
            .load()
            .staged
            .load();
        assert_eq!(wk.tags.len(), 1);
        assert_eq!(wk.lookup.get("dup"), Some(&t_id));
    }

    #[tokio::test]
    async fn create_tag() {
        let (mut state_service, wk_id) = setup_state_service("create-tag").await;

        let t_priority: u64 = 16;
        let t_name = "tag_name".to_string();

        let t_id = state_service
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), None)
            .await
            .unwrap();

        let state_service_pinned = state_service.sync_states.pin();

        let wk = state_service_pinned
            .get(&wk_id)
            .unwrap()
            .load()
            .staged
            .load();

        let tag = wk.tags.get(&t_id);

        assert!(tag.is_some());

        let tag = tag.unwrap().load();
        assert_eq!(tag.name.get().as_str(), t_name.as_str());
        assert_eq!(*tag.priority.get(), t_priority);
        assert_eq!(*tag.parent.get(), None);
        let prev_tag = tag;

        let parent = Some(t_id);
        let t_priority = 10;
        let t_name = "child_tag_name".to_string();
        let mut state_service = state_service.clone();
        let t_id = state_service
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), parent)
            .await
            .unwrap();

        let wk = state_service_pinned
            .get(&wk_id)
            .unwrap()
            .load()
            .staged
            .load();

        let tag = wk.tags.get(&t_id);
        assert!(tag.is_some());
        let tag = tag.unwrap().load();
        assert_eq!(tag.name.get().as_str(), t_name.as_str());
        assert_eq!(*tag.priority.get(), t_priority);
        assert_eq!(*tag.parent.get(), parent);

        let parent_tag = wk.tags.get(&tag.parent.get().unwrap()).unwrap().load();
        assert_eq!(parent_tag.as_ref(), prev_tag.as_ref());
    }

    #[tokio::test]
    async fn delete_tag() {
        let (mut state_service, wk_id) = setup_state_service("delete-tag").await;

        let t_priority: u64 = 16;
        let t_name = "tag_name".to_string();

        let t_id = state_service
            .workspace(wk_id)
            .create_tag(t_priority, t_name.clone(), None)
            .await
            .unwrap();

        let _ = state_service
            .workspace(wk_id)
            .delete_tag(t_id)
            .await
            .unwrap();
        let wk_pin = state_service.sync_states.pin().get(&wk_id).unwrap().load();
        let wk = wk_pin.staged.load();

        let tag = wk.tags.get(&t_id);

        assert!(tag.is_none());

        // [TODO] handle parents
    }

    #[tokio::test]
    async fn assign_shelf() {
        let (mut state_service, wk_id) = setup_state_service("assign-shelf").await;
        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let test_path = std::env::temp_dir().join("ebi-state");

        let shelf_name = "shelf_name".to_string();
        let shelf_description = "shelf_description".to_string();
        let remote = false;

        let s_0_id = state_service
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

        let wk_pin = state_service.sync_states.pin().get(&wk_id).unwrap().load();
        let wk = wk_pin.staged.load();
        let s = wk.shelves.get(&s_0_id);

        assert!(s.is_some());
        let s = s.unwrap();
        assert_eq!(s.info.load().name.get(), &shelf_name);
        assert_eq!(s.info.load().description.get(), &shelf_description);
        assert_eq!(s.shelf_type, ShelfType::Local);
        assert_eq!(s.shelf_owner, ShelfOwner::Node(node_id));

        let shelves_pin = state_service.shelves.pin();
        let s_ref = shelves_pin.get(&s_0_id).unwrap();

        let s_ref = ImmutRef::<Shelf>::upgraded(&s_ref).unwrap();
        assert!(ptr_eq(s_ref.data_ref(), s.data_ref()));
        assert_eq!(s_ref.id, s.id);
    }

    #[tokio::test]
    async fn unassign_shelf() {
        let (mut state_service, wk_id) = setup_state_service("unassign-shelf").await;
        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let test_path = std::env::temp_dir().join("ebi-state");

        let shelf_name = "shelf_name".to_string();
        let shelf_description = "shelf_description".to_string();
        let remote = false;

        let s_0_id = state_service
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

        let _ = state_service
            .workspace(wk_id)
            .unassign_shelf(s_0_id)
            .await
            .unwrap();

        let wk_pin = state_service.sync_states.pin().get(&wk_id).unwrap().load();
        let wk = wk_pin.staged.load();
        let s = wk.shelves.get(&s_0_id);
        assert!(s.is_none());

        let shelves_pin = state_service.shelves.pin();
        let s_ref = shelves_pin.get(&s_0_id).unwrap();
        assert!(ImmutRef::<Shelf>::upgraded(s_ref).is_none());
    }

    #[tokio::test]
    async fn edit_workspace_info() {
        let (mut state_service, wk_id) = setup_state_service("edit-workspace-info").await;
        let up_name = "updated_name".to_string();
        let up_desc = "updated_desc".to_string();
        state_service
            .workspace(wk_id)
            .edit_workspace_info(up_name.clone(), up_desc.clone())
            .await
            .unwrap();

        let wk_pin = state_service.sync_states.pin().get(&wk_id).unwrap().load();
        let wk = wk_pin.staged.load();

        assert_eq!(wk.info.load().name.get(), &up_name);
        assert_eq!(wk.info.load().description.get(), &up_desc);
    }

    // `ShelfInfo`'s `PartialEq` compares values only, so `save_restore_db` doesn't check if the stamps survived

    #[tokio::test]
    async fn workspace_info_field_independence() {
        let (db_path, wk_id, before) = {
            let (mut state_service, wk_id) = setup_state_service("wk-info-stamps").await;

            let revs_of = |ss: &StateService| {
                let i = ss
                    .sync_states
                    .pin()
                    .get(&wk_id)
                    .unwrap()
                    .load()
                    .staged
                    .load()
                    .info
                    .load();
                (i.name.rev(), i.description.rev())
            };

            let before = revs_of(&state_service);

            state_service
                .workspace(wk_id)
                .edit_workspace_info("renamed".to_string(), "none".to_string())
                .await
                .unwrap();

            let after = revs_of(&state_service);
            assert!(
                after.0 > before.0
            );
            assert_eq!(
                after.1, before.1
            );

            (
                std::env::temp_dir()
                    .join("ebi-state")
                    .join("wk-info-stamps")
                    .join("database.redb"),
                wk_id,
                after,
            )
        };

        let reloaded = StateService::full_load(&db_path).unwrap();
        let pin = reloaded.sync_states.pin();
        let wk = pin.get(&wk_id).unwrap().load().staged.load();
        let info = wk.info.load();

        assert_eq!(info.name.get(), "renamed");
        assert_eq!(
            (info.name.rev(), info.description.rev()),
            before
        );
    }

    #[tokio::test]
    async fn shelf_info_field_independence() {
        let (db_path, wk_id, s_id, before) = {
            let (mut state_service, wk_id) = setup_state_service("shelf-info-stamps").await;
            let node_id = NodeId::from_str(TEST_PKEY).unwrap();
            let root = std::env::temp_dir().join("ebi-state");

            let s_id = state_service
                .workspace(wk_id)
                .assign_shelf(
                    root.clone(),
                    node_id,
                    false,
                    Some("shelf".to_string()),
                    Some("desc".to_string()),
                )
                .await
                .unwrap();

            let revs_of = |ss: &StateService| {
                let i = ss
                    .sync_states
                    .pin()
                    .get(&wk_id)
                    .unwrap()
                    .load()
                    .staged
                    .load()
                    .shelves
                    .get(&s_id)
                    .unwrap()
                    .info
                    .load();
                (i.name.rev(), i.description.rev())
            };

            let start = revs_of(&state_service);

            state_service
                .workspace(wk_id)
                .edit_shelf_info(s_id, "shelf".to_string(), "new desc".to_string())
                .await
                .unwrap();

            let after = revs_of(&state_service);
            assert!(after.1 > start.1);
            assert_eq!(after.0, start.0);

            (
                std::env::temp_dir()
                    .join("ebi-state")
                    .join("shelf-info-stamps")
                    .join("database.redb"),
                wk_id,
                s_id,
                after,
            )
        };

        let reloaded = StateService::full_load(&db_path).unwrap();
        let pin = reloaded.sync_states.pin();
        let wk = pin.get(&wk_id).unwrap().load().staged.load();
        let shelf = wk.shelves.get(&s_id).unwrap();
        let info = shelf.info.load();

        assert_eq!(info.description.get(), "new desc");
        assert_eq!((info.name.rev(), info.description.rev()), before);
        assert_eq!(info.root, std::env::temp_dir().join("ebi-state"));
    }

    #[tokio::test]
    async fn edit_shelf_info() {
        let (mut state_service, wk_id) = setup_state_service("edit-shelf-info").await;
        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let test_path = std::env::temp_dir().join("ebi-state");
        let shelf_name = "shelf_name".to_string();
        let shelf_description = "shelf_description".to_string();
        let remote = false;

        let s_id = state_service
            .workspace(wk_id)
            .assign_shelf(
                test_path.clone(),
                node_id,
                remote,
                Some(shelf_name),
                Some(shelf_description),
            )
            .await
            .unwrap();

        let up_name = "updated_name".to_string();
        let up_desc = "updated_desc".to_string();

        let _ = state_service
            .workspace(wk_id)
            .edit_shelf_info(s_id, up_name.clone(), up_desc.clone())
            .await
            .unwrap();

        let wk_pin = state_service.sync_states.pin().get(&wk_id).unwrap().load();
        let wk = wk_pin.staged.load();
        let s = wk.shelves.get(&s_id).unwrap();
        assert_eq!(s.info.load().name.get(), &up_name);
        assert_eq!(s.info.load().description.get(), &up_desc);
    }
}
