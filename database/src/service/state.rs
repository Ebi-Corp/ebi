use crate::redb::*;
use crate::service::scoped::ScopedDatabase;
use crate::{Shelf, Workspace};
use ::redb::Database;
use arc_swap::ArcSwap;
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::shelf::*;
use ebi_types::workspace::{WorkspaceId, WorkspaceInfo};
use ebi_types::{Uuid, sharedref::*, stateful::*};
use redb::{Error, ReadableTable};
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::Service;

#[derive(Debug)]
pub struct GroupState {
    pub workspaces: StatefulMap<WorkspaceId, Arc<StatefulRef<Workspace>>>,
    pub shelves: StatefulMap<ShelfId, WeakRef<Shelf>>,
}

impl Default for GroupState {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupState {
    pub fn new() -> Self {
        Self {
            workspaces: StatefulMap::new(SwapRef::new_ref(())),
            shelves: StatefulMap::new(SwapRef::new_ref(())),
        }
    }
}

#[derive(Clone)]
pub struct StateDatabase {
    pub state: Arc<ArcSwap<History<GroupState>>>,
    pub lock: Arc<RwLock<()>>,
    pub db: Arc<Database>,
}

impl StateDatabase {
    pub fn new(db_path: &PathBuf) -> Result<Self, Error> {
        let lock = Arc::new(RwLock::new(()));
        let db = Database::create(db_path)?;
        let state = Arc::new(ArcSwap::new(History::new(GroupState::new()).into()));
        let write_txn = db.begin_write()?;
        let loaded_state = state.load();
        write_txn.open_table(T_ENTITY_STATE)?.insert(
            loaded_state.staged.id,
            std::collections::HashMap::new().to_storable(),
        )?;
        write_txn
            .open_table(T_STATE_STATUS)?
            .insert(loaded_state.staged.id, StateStatus::Staged.to_storable())?;
        write_txn.commit()?;

        Ok(Self {
            state,
            lock: lock.clone(),
            db: Arc::new(db),
        })
    }

    pub async fn sync_state(&mut self) {
        let lock = self.lock.write().await;
        let hist = self.state.load();
        let (new_hist, rem_state) = hist.next();
        let write_txn = self.db.begin_write().unwrap();
        let prev_staged = &hist.staged;
        let new_staged = &new_hist.staged;
        {
            let mut state_t = write_txn.open_table(T_STATE_STATUS).unwrap();
            state_t
                .insert(new_staged.id, StateStatus::Staged.to_storable())
                .unwrap();
            state_t
                .insert(prev_staged.id, StateStatus::Synced.to_storable())
                .unwrap();

            if let Some(synced) = &hist.synced {
                let ord = {
                    if let Some(past_state) = hist.hist.front() {
                        match state_t.get(past_state.id).unwrap().unwrap().value().0 {
                            StateStatus::History(val) => val - 1,
                            _ => unreachable!(),
                        }
                    } else {
                        std::u64::MAX
                    }
                };
                state_t
                    .insert(synced.id, StateStatus::History(ord).to_storable())
                    .unwrap();
            }
            let mut entity_t = write_txn.open_table(T_ENTITY_STATE).unwrap();
            let staged = entity_t
                .get(prev_staged.id)
                .unwrap()
                .unwrap()
                .value()
                .0
                .clone();
            entity_t
                .insert(new_staged.id, staged.to_storable())
                .unwrap();

            if let Some(rem_id) = rem_state {
                state_t.remove(rem_id).unwrap();
                let _old_map = entity_t.remove(rem_id).unwrap();
                // [TODO] delete elements that are in old map but
                // not contained in next state
            }
        }
        write_txn.commit().unwrap();
        self.state.store(Arc::new(new_hist));
        drop(lock);
    }

    pub fn workspace(&mut self, id: WorkspaceId) -> ScopedDatabase {
        ScopedDatabase {
            service: self.clone(),
            workspace_scope: id,
        }
    }

    pub async fn get_workspace(
        &mut self,
        id: WorkspaceId,
    ) -> Result<ImmutRef<Workspace>, ReturnCode> {
        self.call(GetWorkspace { id }).await
    }

    pub async fn create_workspace(
        &mut self,
        name: String,
        description: String,
    ) -> Result<WorkspaceId, ReturnCode> {
        self.call(CreateWorkspace { name, description }).await
    }
    pub async fn get_workspaces(&mut self) -> Result<Vec<ebi_proto::rpc::Workspace>, ReturnCode> {
        self.call(GetWorkspaces {}).await
    }
    pub async fn remove_workspace(&mut self, workspace_id: WorkspaceId) -> Result<(), ReturnCode> {
        self.call(RemoveWorkspace { workspace_id }).await
    }
}

struct GetWorkspace {
    id: WorkspaceId,
}

impl Service<GetWorkspace> for StateDatabase {
    type Response = ImmutRef<Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspace) -> Self::Future {
        let state = self.state.load().staged.load();
        Box::pin(async move {
            if let Some(workspace) = state.workspaces.get(&req.id) {
                let immut_ref = ImmutRef::new(workspace.id, workspace.load_full());
                Ok(immut_ref)
            } else {
                Err(ReturnCode::WorkspaceNotFound)
            }
        })
    }
}

struct CreateWorkspace {
    pub name: String,
    pub description: String,
}

impl Service<CreateWorkspace> for StateDatabase {
    type Response = WorkspaceId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let state = self.state.load_full();
        let db = self.db.clone();
        Box::pin(async move {
            let w_state = SwapRef::new_ref(()); // [TODO] Spawn bloom filters
            let workspace = Workspace {
                info: StatefulRef::new_ref(WorkspaceInfo::new(
                    Some(req.name),
                    Some(req.description),
                )),
                shelves: StatefulMap::new(w_state.clone()), // Placeholder for local shelves
                tags: StatefulMap::new(w_state.clone()),
                lookup: StatefulMap::new(w_state.clone()),
            };
            let w_ref = StatefulRef::new_ref(workspace);
            let w_id = w_ref.id;

            state
                .staged
                .stateful_rcu(|s| {
                    let (u_m, u_s) = s.workspaces.insert(w_id, w_ref.clone_inner().into());
                    let u_w = GroupState {
                        workspaces: u_m,
                        shelves: s.shelves.clone(),
                    };
                    (u_w, u_s)
                })
                .await;
            let staged_id = state.staged.id;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let db_id = Uuid::new_v4();
                wk_t.insert(db_id, w_ref.to_storable()).unwrap();
                state_hmap.0.insert(w_id, (db_id, true));
                entity_state_t.insert(staged_id, state_hmap).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;

            Ok(w_id)
        })
    }
}

struct GetWorkspaces {}

impl Service<GetWorkspaces> for StateDatabase {
    type Response = Vec<ebi_proto::rpc::Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: GetWorkspaces) -> Self::Future {
        let state = self.state.load().staged.load();
        Box::pin(async move {
            let mut workspace_ls = Vec::new();
            for (_, workspace) in state.workspaces.iter() {
                let mut tag_ls = Vec::new();
                for tag in workspace.load().tags.values() {
                    let tag_id = tag.id;
                    let tag = tag.load();
                    let name = tag.name.clone();
                    let priority = tag.priority;
                    let parent_id = tag
                        .parent
                        .clone()
                        .map(|parent| parent.id.as_bytes().to_vec());
                    tag_ls.push(ebi_proto::rpc::Tag {
                        tag_id: tag_id.as_bytes().to_vec(),
                        name,
                        priority,
                        parent_id,
                    });
                }
                let workspace_id = workspace.id;
                let workspace = workspace.load();
                let wk_info = workspace.info.load();
                let ws = ebi_proto::rpc::Workspace {
                    workspace_id: workspace_id.as_bytes().to_vec(),
                    name: wk_info.name.get().clone(),
                    description: wk_info.description.get().clone(),
                    tags: tag_ls,
                };
                workspace_ls.push(ws);
            }
            Ok(workspace_ls)
        })
    }
}

struct RemoveWorkspace {
    pub workspace_id: WorkspaceId,
}

impl Service<RemoveWorkspace> for StateDatabase {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveWorkspace) -> Self::Future {
        let g_state = self.state.load_full();
        let db = self.db.clone();
        Box::pin(async move {
            g_state
                .staged
                .stateful_rcu(|s| {
                    let (u_m, u_s) = s.workspaces.remove(&req.workspace_id);
                    let u_g = GroupState {
                        shelves: s.shelves.clone(),
                        workspaces: u_m,
                    };
                    (u_g, u_s)
                })
                .await;
            let staged_id = g_state.staged.id;
            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut _wk_t = write_txn
                    .open_table(T_WKSPC)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut entity_state_t = write_txn
                    .open_table(T_ENTITY_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut state_hmap = entity_state_t.get(staged_id).unwrap().unwrap().value();
                let (_db_id, _) = state_hmap
                    .0
                    .get(&req.workspace_id)
                    .ok_or(ReturnCode::InternalStateError)?;
                // in order to delete, need to check if any state contains the wk
                /*
                wk_t.remove(db_id)
                    .map_err(|_| ReturnCode::InternalStateError)?;
                */
                state_hmap.0.remove(&req.workspace_id).unwrap();
                entity_state_t.insert(staged_id, state_hmap).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;
            Ok(())
        })
    }
}
