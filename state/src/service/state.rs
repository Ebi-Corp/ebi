use crate::SyncState;
use crate::Workspace;
use crate::redb::*;
use crate::service::workspace::WorkspaceStateService;
use ::redb::Database;
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::workspace::{WorkspaceId, WorkspaceInfo};
use ebi_types::{Uuid, sharedref::*, stateful::*};
use papaya::HashSet;
use redb::{Error, ReadableTable};
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::Service;

#[derive(Clone)]
pub struct StateService {
    pub sync_states: Arc<HashSet<SharedRef<SyncState<Workspace>>>>,
    pub db: Arc<Database>,
}

impl StateService {
    pub fn new(db_path: &PathBuf) -> Result<Self, Error> {
        let db = Database::create(db_path)?;
        let sync_states = Arc::new(HashSet::new().into());

        Ok(Self {
            sync_states,
            db: Arc::new(db),
        })
    }

    pub fn workspace(&mut self, id: WorkspaceId) -> WorkspaceStateService {
        WorkspaceStateService {
            service: self.clone(),
            scope: id,
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

impl Service<GetWorkspace> for StateService {
    type Response = ImmutRef<Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspace) -> Self::Future {
        let sync_states = self.sync_states.clone();
        Box::pin(async move {
            let sync_states = sync_states.pin();
            if let Some(wks) = sync_states.get(&req.id) {
                let wks = wks.load();
                let wk_staged = &wks.staged;
                Ok(ImmutRef::new(wk_staged.id, wk_staged.load_full()))
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

// This call handles the creation of the VersionStates containing the new workspace.
// Since each workspace is associated to a single network, network creation must be handled at
// the respective abstraction level
impl Service<CreateWorkspace> for StateService {
    type Response = WorkspaceId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let sync_states = self.sync_states.clone();
        let db = self.db.clone();
        Box::pin(async move {
            let w_state = SharedRef::new_ref((), ()); // [TODO] Spawn bloom filters
            let workspace = Workspace {
                info: StatefulRef::new_ref(
                    (),
                    WorkspaceInfo::new(Some(req.name), Some(req.description)),
                ),
                shelves: StatefulMap::new(w_state.clone()), // Placeholder for local shelves
                tags: StatefulMap::new(w_state.clone()),
                lookup: StatefulMap::new(w_state.clone()),
            };
            let sync_state = SharedRef::new_ref(Uuid::new_v4(), SyncState::new(workspace));

            let sync_state_id = sync_state.id;
            let sync_state_ref = sync_state.load();

            let staged_id = sync_state_ref.staged.id;
            let w_ref = &sync_state_ref.staged;

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut sync_state_t = write_txn
                    .open_table(T_SYNC_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                sync_state_t
                    .insert(
                        sync_state_id,
                        vec![WorkspaceState::new(staged_id, StateStatus::Staged).to_storable()],
                    )
                    .map_err(|_| ReturnCode::DbCommitError)?;
                workspace_t
                    .insert(staged_id, w_ref.to_storable())
                    .map_err(|_| ReturnCode::DbCommitError)?;
                workspace_entities_t
                    .insert(staged_id, Vec::new())
                    .map_err(|_| ReturnCode::DbCommitError)?;
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;
            sync_states.pin().insert(sync_state);

            Ok(sync_state_id)
        })
    }
}

struct GetWorkspaces {}

impl Service<GetWorkspaces> for StateService {
    type Response = Vec<ebi_proto::rpc::Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: GetWorkspaces) -> Self::Future {
        let sync_states = self.sync_states.clone();
        Box::pin(async move {
            let mut workspace_ls = Vec::new();
            let sync_states = sync_states.pin();
            for g_state in sync_states.iter() {
                let workspace = &g_state.load().staged.load();
                let mut tag_ls = Vec::new();
                for tag in workspace.tags.values() {
                    let tag_id = tag.id;
                    let tag = tag.load();
                    let name = tag.name.get().clone();
                    let priority = *tag.priority.get();
                    let parent_id = tag.parent.get().map(|parent| parent.as_bytes().to_vec());
                    tag_ls.push(ebi_proto::rpc::Tag {
                        tag_id: tag_id.as_bytes().to_vec(),
                        name,
                        priority,
                        parent_id,
                    });
                }
                let workspace_id = g_state.id;
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

impl Service<RemoveWorkspace> for StateService {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveWorkspace) -> Self::Future {
        let sync_states = self.sync_states.clone();
        let db = self.db.clone();
        Box::pin(async move {
            let sync_states = sync_states.pin();
            let Some(sync_state) = sync_states.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };
            sync_states.remove(&req.workspace_id);

            let write_txn = db.begin_write().map_err(|_| ReturnCode::DbOpenError)?;
            {
                let mut sync_state_t = write_txn
                    .open_table(T_SYNC_STATE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_t = write_txn
                    .open_table(T_WORKSPACE)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;
                let mut workspace_entities_t = write_txn
                    .open_table(T_WORKSPACE_ENTITIES)
                    .map_err(|_| ReturnCode::DbTableOpenError)?;

                // note: currently removing a workspace leaves orphaned T_SHELF and T_TAG
                let states = sync_state_t.get(sync_state.id).unwrap().unwrap().value();

                for state in states {
                    workspace_t
                        .remove(state.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                    workspace_entities_t
                        .remove(state.0.id)
                        .map_err(|_| ReturnCode::DbCommitError)?;
                }
                sync_state_t.remove(sync_state.id).unwrap();
            }
            write_txn.commit().map_err(|_| ReturnCode::DbCommitError)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_workspace() {
        let test_path = std::env::temp_dir().join("ebi-state");
        let test_path = test_path.join("create-workspace");
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

        let state_wk_pin = state_service.sync_states.pin();
        let wk = state_wk_pin.get(&wk_id);

        assert!(&wk.is_some());
        let wk = wk.unwrap();
        assert_eq!(wk.load().staged.load().info.load().name.get(), "workspace");
        assert_eq!(
            wk.load().staged.load().info.load().description.get(),
            "none"
        );
    }

    #[tokio::test]
    async fn remove_workspace() {
        let test_path = std::env::temp_dir().join("ebi-state");
        let test_path = test_path.join("remove-workspace");
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

        let _ = state_service.remove_workspace(wk_id).await.unwrap();

        let state_wk_pin = state_service.sync_states.pin();
        let wk = state_wk_pin.get(&wk_id);

        assert!(&wk.is_none());
    }
}
