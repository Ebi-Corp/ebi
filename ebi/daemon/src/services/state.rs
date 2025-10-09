pub mod prelude {
    pub use super::AssignShelf;
    pub use super::GetWorkspace;
    pub use super::RemoveWorkspace;
    pub use super::StateService;
    pub use super::UnassignShelf;
}

use crate::prelude::*;
use crate::services::filesys::FileSysService;
use crate::sharedref::History;
use crate::shelf::{Shelf, ShelfId, ShelfOwner};
use crate::stateful::{StatefulMap, SwapRef};
use crate::tag::Tag;
use crate::workspace::{Workspace, WorkspaceId, WorkspaceInfo};
use ebi_proto::rpc::*;
use iroh::NodeId;
use std::path::PathBuf;
use std::sync::Weak;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::Service;

#[derive(Clone)]
pub struct StateService {
    pub state: Arc<History<GroupState>>,
    pub lock: Arc<RwLock<()>>,
    pub filesys: FileSysService,
}

impl StateService {
    pub fn new(filesys: FileSysService) -> Self {
        let lock = Arc::new(RwLock::new(()));
        let state = Arc::new(History::new(GroupState::new(), lock.clone()));
        Self {
            state,
            lock: lock.clone(),
            filesys: filesys.clone(),
        }
    }
}

pub struct GroupState {
    pub workspaces: StatefulMap<WorkspaceId, Arc<StatefulRef<Workspace>>>,
    pub shelf_assignment: StatefulMap<ShelfId, Vec<Weak<StatefulRef<Workspace>>>>,
}
impl GroupState {
    fn new() -> Self {
        Self {
            workspaces: StatefulMap::new(SwapRef::new(())),
            shelf_assignment: StatefulMap::new(SwapRef::new(())),
        } //[!] Add Bloom Filter
    }
}

enum Operations {
    GetWorkspace(WorkspaceId),
}

pub struct GetWorkspace {
    pub id: WorkspaceId,
}

impl Service<GetWorkspace> for StateService {
    type Response = Arc<StatefulRef<Workspace>>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspace) -> Self::Future {
        let state = self.state.staged.load(); // [!] The read state should be configurable, but always write to staged
        Box::pin(async move {
            if let Some(workspace) = state.workspaces.get(&req.id) {
                Ok(workspace.clone())
            } else {
                Err(ReturnCode::WorkspaceNotFound)
            }
        })
    }
}

pub struct CreateWorkspace {
    pub name: String,
    pub description: String,
}

impl Service<CreateWorkspace> for StateService {
    type Response = WorkspaceId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let state = self.state.clone();
        let lock = self.lock.clone();
        Box::pin(async move {
            let w_state = SwapRef::new(()); // [TODO] Spawn bloom filters
            let workspace = Workspace {
                info: StatefulRef::new_ref(
                    WorkspaceInfo::new(Some(req.name), Some(req.description)),
                    lock.clone(),
                ),
                shelves: StatefulMap::new(w_state.clone()), // Placeholder for local shelves
                tags: StatefulMap::new(w_state.clone()),
                lookup: StatefulMap::new(w_state.clone()),
            };
            let w_ref = StatefulRef::new_ref(workspace, lock.clone());
            let w_id = w_ref.id;

            state
                .staged
                .stateful_rcu(|s| {
                    let (u_m, u_s) = s.workspaces.insert(w_id, w_ref.clone().into());
                    let u_w = GroupState {
                        workspaces: u_m,
                        shelf_assignment: s.shelf_assignment.clone(),
                    };
                    (u_w, u_s)
                })
                .await;

            Ok(w_id)
        })
    }
}

pub struct CreateTag {
    pub priority: u64,
    pub name: String,
    pub parent: Option<SharedRef<Tag>>,
}

impl Service<CreateTag> for StateService {
    type Response = SharedRef<Tag>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let _lock = self.lock.clone(); // probably need to take read lock
        Box::pin(async move {
            let tag = Tag {
                priority: req.priority,
                name: req.name,
                parent: req.parent,
            };
            let tag_ref = SharedRef::new_ref(tag);

            // [TODO] notice the create filter
            Ok(tag_ref)
        })
    }
}

pub struct GetWorkspaces {}

impl Service<GetWorkspaces> for StateService {
    type Response = Vec<ebi_proto::rpc::Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: GetWorkspaces) -> Self::Future {
        let state = self.state.staged.load();
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
                let workspace_id = workspace.id();
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

pub struct UnassignShelf {
    pub shelf_id: ShelfId,
    pub workspace_id: WorkspaceId,
}

impl Service<UnassignShelf> for StateService {
    type Response = (); // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnassignShelf) -> Self::Future {
        let g_state = self.state.clone();
        Box::pin(async move {
            let state = g_state.staged.load();
            let Some(_) = state.shelf_assignment.get(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };
            let Some(wk) = state.workspaces.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            wk.stateful_rcu(|w| {
                let (u_m, u_s) = w.shelves.remove(&req.shelf_id);
                let u_w = Workspace {
                    info: w.info.clone(),
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

pub struct AssignShelf {
    pub path: PathBuf,
    pub node_id: NodeId,
    pub remote: bool,
    pub description: Option<String>,
    pub name: Option<String>,
    pub workspace_id: WorkspaceId,
}

impl Service<AssignShelf> for StateService {
    //[?] Create Shelf during assignment (if it does not exist) ?? //[/] Would help with having ShelfInfo in history
    type Response = ShelfId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AssignShelf) -> Self::Future {
        let g_state = self.state.clone();
        let lock = self.lock.clone();
        let mut fs = self.filesys.clone();

        Box::pin(async move {
            let state = g_state.staged.load();
            let Some(workspace) = state.workspaces.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            // The ID is deterministically created on node + path
            let bytes = [
                &req.node_id.as_bytes()[0..8],
                req.path.to_str().unwrap().as_bytes(),
            ]
            .concat();
            let shelf_id = Uuid::new_v5(&Uuid::NAMESPACE_DNS, &bytes);

            let mut new_shelf: bool = true;
            let mut shelf_ref: Option<ImmutRef<Shelf>> = None; // workaround for compiler, is not
            // aware of guaranteed intialization

            if let Some(v_wk) = state.shelf_assignment.get(&shelf_id) {
                for w_ref in v_wk {
                    match w_ref.upgrade() {
                        Some(w_ref) => {
                            shelf_ref = Some(w_ref.load().shelves.get(&shelf_id).unwrap().clone());
                            new_shelf = false;
                            break;
                        }
                        None => {
                            // clean references if workspace ref upgrade failed.
                            g_state
                                .staged
                                .stateful_rcu(|w| {
                                    let (u_m, u_s) = w.shelf_assignment.remove(&shelf_id);
                                    let u_w = GroupState {
                                        shelf_assignment: u_m,
                                        workspaces: w.workspaces.clone(),
                                    };
                                    (u_w, u_s)
                                })
                                .await;
                        }
                    }
                }
            }

            if new_shelf {
                let path = req.path.clone();
                let root_shelf_ref = if !req.remote {
                    Some(fs.get_or_init_shelf(path.clone()).await?)
                } else {
                    None
                };
                let Ok(shelf) = Shelf::new(
                    lock,
                    path,
                    root_shelf_ref,
                    req.name.unwrap_or_else(|| {
                        req.path
                            .clone()
                            .components()
                            .next_back()
                            .and_then(|comp| comp.as_os_str().to_str())
                            .unwrap_or_default()
                            .to_string()
                    }),
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

            workspace
                .stateful_rcu(|w| {
                    let (u_m, u_s) = w.shelves.insert(shelf_id, shelf_ref.clone());
                    let u_w = Workspace {
                        info: w.info.clone(),
                        shelves: u_m,
                        tags: w.tags.clone(),
                        lookup: w.lookup.clone(),
                    };
                    (u_w, u_s)
                })
                .await;

            let w_ref = Arc::downgrade(workspace);

            if new_shelf {
                g_state
                    .staged
                    .stateful_rcu(|s| {
                        let (u_m, u_s) = s
                            .shelf_assignment
                            .insert(shelf_id, Vec::from([w_ref.clone()]));
                        let u_g = GroupState {
                            shelf_assignment: u_m,
                            workspaces: s.workspaces.clone(),
                        };
                        (u_g, u_s)
                    })
                    .await;
            } else {
                g_state
                    .staged
                    .stateful_rcu(|s| {
                        let mut vec = s.shelf_assignment.get(&shelf_id).unwrap().clone();
                        vec.push(w_ref.clone());
                        let (u_m, u_s) = s.shelf_assignment.insert(shelf_id, vec);
                        let u_g = GroupState {
                            shelf_assignment: u_m,
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

pub struct RemoveWorkspace {
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
        let g_state = self.state.clone();
        Box::pin(async move {
            g_state
                .staged
                .stateful_rcu(|s| {
                    let (u_m, u_s) = s.workspaces.remove(&req.workspace_id);
                    let u_g = GroupState {
                        shelf_assignment: s.shelf_assignment.clone(),
                        workspaces: u_m,
                    };
                    (u_g, u_s)
                })
                .await;
            Ok(())
        })
    }
}
