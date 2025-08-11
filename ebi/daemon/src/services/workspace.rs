use crate::shelf::{Shelf, ShelfId, ShelfOwner, ShelfRef};
use crate::tag::{TagId, TagRef};
use crate::workspace::{Workspace, WorkspaceId, WorkspaceRef};
use ebi_proto::rpc::*;
use iroh::NodeId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::Service;
use uuid::Uuid;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

#[derive(Clone, Debug)]
pub struct WorkspaceService {
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceRef>>>,
    pub shelf_assignment: Arc<RwLock<HashMap<ShelfId, Vec<WorkspaceId>>>>,
    pub paths: Arc<RwLock<HashMap<NodeId, HashMap<PathBuf, ShelfId>>>>, // [?] Should this be HashMap<NodeId, HashMap<PathBuf, ShelfId>> ?? //[/] Node IDs can be gathered from the ShelfRef
}

enum Operations {
    GetWorkspace(WorkspaceId),
}

pub struct GetWorkspace {
    pub id: WorkspaceId,
}

impl Service<GetWorkspace> for WorkspaceService {
    type Response = WorkspaceRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            if let Some(workspace) = workspaces.read().await.get(&req.id) {
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

impl Service<CreateWorkspace> for WorkspaceService {
    type Response = WorkspaceId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let id = Uuid::now_v7();

            let workspace = Workspace {
                id,
                name: req.name,
                description: req.description,
                shelves: HashMap::new(), // Placeholder for local shelves
                tags: HashMap::new(),
                lookup: HashMap::new(),
            };

            workspaces
                .write()
                .await
                .insert(id, Arc::new(RwLock::new(workspace)));

            Ok(id)
        })
    }
}

pub struct GetWorkspaces {}

impl Service<GetWorkspaces> for WorkspaceService {
    type Response = Vec<ebi_proto::rpc::Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: GetWorkspaces) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let mut workspace_ls = Vec::new();
            for (_, workspace) in workspaces.read().await.iter() {
                let mut tag_ls = Vec::new();
                for tag_ref in workspace.read().await.tags.values() {
                    let tag = tag_ref.tag_ref.read().unwrap();
                    let tag_id = tag.id;
                    let name = tag.name.clone();
                    let priority = tag.priority;
                    let parent_id = match tag.parent.clone() {
                        Some(parent) => Some(parent.tag_ref.read().unwrap().id.as_bytes().to_vec()),
                        None => None,
                    };
                    tag_ls.push(Tag {
                        tag_id: tag_id.as_bytes().to_vec(),
                        name,
                        priority,
                        parent_id,
                    });
                }
                let workspace = workspace.read().await;
                let ws = ebi_proto::rpc::Workspace {
                    workspace_id: workspace.id.as_bytes().to_vec(),
                    name: workspace.name.clone(),
                    description: workspace.description.clone(),
                    tags: tag_ls,
                };
                workspace_ls.push(ws);
            }
            Ok(workspace_ls)
        })
    }
}

pub struct GetShelf {
    pub shelf_id: ShelfId,
    pub workspace_id: WorkspaceId,
}

impl Service<GetShelf> for WorkspaceService {
    type Response = ShelfRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let workspace_r = workspaces.read().await;
            let Some(workspace) = workspace_r.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            if let Some(shelf) = workspace.clone().read().await.shelves.get(&req.shelf_id) {
                Ok(shelf.clone())
            } else {
                Err(ReturnCode::ShelfNotFound)
            }
        })
    }
}

pub struct UnassignShelf {
    pub shelf_id: ShelfId,
    pub workspace_id: WorkspaceId,
}

impl Service<UnassignShelf> for WorkspaceService {
    type Response = bool; // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnassignShelf) -> Self::Future {
        let shelf_assignment = self.shelf_assignment.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            let mut shelf_assignment_w = shelf_assignment.write().await;
            let Some(workspace_list) = shelf_assignment_w.get_mut(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound);
            };
            workspace_list.retain(|&w_id| w_id != req.workspace_id);
            if workspace_list.is_empty() {
                for local_paths in paths.write().await.values_mut() {
                    local_paths.retain(|_, s_id| s_id != &req.shelf_id);
                }
                return Ok(true);
            }
            Ok(false)
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

impl Service<AssignShelf> for WorkspaceService {
    type Response = ShelfId; // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // [!] Need to analyze lock usage of this fn in parallel with other functions
    fn call(&mut self, req: AssignShelf) -> Self::Future {
        let shelf_assignment = self.shelf_assignment.clone();
        let paths = self.paths.clone();
        let workspaces = self.workspaces.clone();

        Box::pin(async move {
            let mut paths_w = paths.write().await;
            let Some(node_path) = paths_w.get_mut(&req.node_id) else {
                return Err(ReturnCode::PeerNotFound);
            };
            let Some(workspace) = workspaces.read().await.get(&req.workspace_id).cloned() else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            let shelf_id: ShelfId;

            let shelf_ref = match node_path.get_mut(&req.path) {
                Some(ex_shelf_id) => {
                    // [/] Unwraps are safe as workspace and shelf id are known to exist here
                    shelf_id = *ex_shelf_id;
                    let workspace_id = *shelf_assignment
                        .read()
                        .await
                        .get(ex_shelf_id)
                        .unwrap()
                        .first()
                        .unwrap();
                    workspaces
                        .read()
                        .await
                        .get(&workspace_id)
                        .unwrap()
                        .read()
                        .await
                        .shelves
                        .get(ex_shelf_id)
                        .unwrap()
                        .clone()
                }
                None => {
                    let path = req.path.clone();
                    let Ok(shelf) = Shelf::new(
                        req.remote,
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
                        ShelfOwner::Node(req.node_id),
                        None,
                        req.description.unwrap_or_default(),
                    ) else {
                        return Err(ReturnCode::ShelfCreationIOError);
                    };
                    shelf_id = shelf.info.id;
                    Arc::new(RwLock::new(shelf))
                }
            };
            workspace
                .clone()
                .write()
                .await
                .shelves
                .insert(shelf_id, shelf_ref);

            let mut shelf_assignment_w = shelf_assignment.write().await;
            shelf_assignment_w
                .entry(shelf_id)
                .and_modify(|v| v.push(req.workspace_id))
                .or_insert(Vec::from(&[req.workspace_id]));

            drop(shelf_assignment_w);

            node_path.entry(req.path).or_insert(shelf_id);

            Ok(shelf_id)
        })
    }
}

pub struct RemoveWorkspace {
    pub workspace_id: WorkspaceId,
}

impl Service<RemoveWorkspace> for WorkspaceService {
    type Response = ();
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let shelf_assignment = self.shelf_assignment.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            let mut shelf_assignment_w = shelf_assignment.write().await;
            for (shelf, workspace_ls) in shelf_assignment_w.iter_mut() {
                workspace_ls.retain(|&w_id| w_id != req.workspace_id);
                if workspace_ls.is_empty() {
                    for local_paths in paths.write().await.values_mut() {
                        local_paths.retain(|_, s_id| s_id != shelf);
                    }
                }
            }
            let mut workspaces_w = workspaces.write().await;
            workspaces_w.remove(&req.workspace_id);
            Ok(())
        })
    }
}

pub struct GetTag {
    pub tag_id: TagId,
    pub workspace_id: WorkspaceId,
}

impl Service<GetTag> for WorkspaceService {
    type Response = TagRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetTag) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let workspace_r = workspaces.read().await;
            let Some(workspace) = workspace_r.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            if let Some(tag) = workspace.clone().read().await.tags.get(&req.tag_id) {
                Ok(tag.clone())
            } else {
                Err(ReturnCode::TagNotFound)
            }
        })
    }
}
