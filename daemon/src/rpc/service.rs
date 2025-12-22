use bincode::serde::encode_to_vec;
use ebi_gstate::service::state::StateDatabase;
use ebi_filesystem::service::{FileSystem, ShelfDirKey};
use ebi_network::service::Network;
use ebi_proto::rpc::*;
use ebi_query::service::QueryService;
use ebi_types::shelf::{ShelfId, ShelfOwner, ShelfType};
use ebi_types::*;
use iroh::NodeId;
use papaya::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;
use tower::Service;

#[derive(Clone)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>,
    pub network: Network,
    pub state_db: StateDatabase,
    pub filesys: FileSystem,
    pub query_srv: QueryService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,

    // [!] This should be Mutexes. reason about read-write ratio
    pub responses: Arc<HashMap<RequestId, Response>>,
    pub broadcast: Sender<Uuid>,
    pub watcher: Receiver<Uuid>,
}
pub type TaskID = u64;

#[derive(Debug)]
pub enum Notification {
    Heartbeat(Heartbeat),
    Operation(Operation),
    PeerConnected(NodeId),
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum DaemonInfoField {
    Name,
}

#[derive(Debug)]
pub struct DaemonInfo {
    pub id: Arc<NodeId>,
    pub name: StatefulField<DaemonInfoField, String>,
}

impl DaemonInfo {
    pub fn new(id: NodeId, name: String) -> Self {
        DaemonInfo {
            id: Arc::new(id),
            name: {
                let field = StatefulField::<DaemonInfoField, String>::new(
                    DaemonInfoField::Name,
                    InfoState::new(),
                );
                let (field, updater) = field.set(&name);
                drop(updater); // No State Update required for Info Creation
                field
            },
        }
    }
}

impl Service<Response> for RpcService {
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Response) -> Self::Future {
        let responses = self.responses.clone();
        let broadcast = self.broadcast.clone();
        Box::pin(async move {
            let res = req;
            let metadata = res.metadata().ok_or(())?;
            let uuid = Uuid::try_from(metadata.request_uuid).map_err(|_| ())?;
            responses.pin().insert(uuid, res);
            broadcast.send(uuid).map_err(|_| ())?;
            Ok(())
        })
    }
}

impl Service<ClientQuery> for RpcService {
    type Response = ClientQueryResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut query_srv = self.query_srv.clone();
        Box::pin(async move {
            let Some(metadata) = req.clone().metadata else {
                return Err(ReturnCode::MalformedRequest)?;
            };

            match query_srv.call(req).await {
                Ok((token, packets)) => Ok(ClientQueryResponse {
                    token: token.as_bytes().to_vec(),
                    packets,
                    metadata: Some(Status {
                        request_uuid: metadata.request_uuid,
                        return_code: ReturnCode::Success as u32,
                        error_data: None,
                    }),
                }),
                Err(ret_code) => Err(Status {
                    request_uuid: metadata.request_uuid,
                    return_code: ret_code as u32,
                    error_data: None,
                }),
            }
        })
    }
}

impl Service<PeerQuery> for RpcService {
    type Response = PeerQueryResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: PeerQuery) -> Self::Future {
        let mut query_srv = self.query_srv.clone();
        Box::pin(async move {
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let (files, errors) = query_srv.call(req).await?;
            if let Ok(files) = encode_to_vec(&files, config).map_err(|_| ReturnCode::ParseError) {
                Ok(PeerQueryResponse {
                    files,
                    metadata: Some(Status {
                        request_uuid: Into::<Vec<u8>>::into(Uuid::new_v4()),
                        return_code: ReturnCode::Success as u32, // [?] Should this always be success ??
                        error_data: Some(ErrorData { error_data: errors }),
                    }),
                })
            } else {
                Err(Status {
                    request_uuid: Into::<Vec<u8>>::into(Uuid::new_v4()),
                    return_code: ReturnCode::PeerServiceError as u32, // [!] Encode Error
                    error_data: Some(ErrorData { error_data: errors }),
                })
            }
        })
    }
}

impl Service<DeleteTag> for RpcService {
    type Response = DeleteTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let mut state_db = self.state_db.clone();
        let mut filesys = self.filesys.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (workspace_id, tag_id) = (uuid(&req.workspace_id)?, uuid(&req.tag_id)?);

            let workspace = state_db.get_workspace(workspace_id).await?;

            let tag_ref = state_db.workspace(workspace_id).delete_tag(tag_id).await?;

            for (_id, shelf) in workspace.shelves.iter() {
                match &shelf.shelf_type {
                    ShelfType::Local => {
                        let shelf_key = ShelfDirKey::Path(shelf.info.load().root.to_path_buf());
                        let _ = filesys.strip_tag(shelf_key, None, tag_ref.clone()).await;

                        //[?] Are (remote) Sync'd shelves also in shelves ??
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                    }
                    ShelfType::Remote => {
                        //[TODO] Remote Request
                    }
                }
            }

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data,
            };
            Ok(DeleteTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<StripTag> for RpcService {
    type Response = StripTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: StripTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let mut network = self.network.clone();
        let mut state_db = self.state_db.clone();
        let mut filesys = self.filesys.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (shelf_id, tag_id, workspace_id) = (
                uuid(&req.shelf_id)?,
                uuid(&req.tag_id)?,
                uuid(&req.workspace_id)?,
            );

            let workspace = state_db.get_workspace(workspace_id).await?;

            let shelf = workspace
                .shelves
                .get(&shelf_id)
                .ok_or(ReturnCode::ShelfNotFound)?;

            let tag = workspace.tags.get(&tag_id).ok_or(ReturnCode::TagNotFound)?;

            let return_code = match &shelf.shelf_type {
                ShelfType::Local => {
                    let shelf_data = filesys
                        .get_or_init_shelf(ShelfDirKey::Path(shelf.info.load().root.to_path_buf()))
                        .await
                        .unwrap();
                    let path = match req.path {
                        Some(path) => Some(
                            std::path::absolute(PathBuf::from(path))
                                .map_err(|_| ReturnCode::PathNotFound)?,
                        ),
                        None => None,
                    };

                    let result = filesys
                        .strip_tag(ShelfDirKey::Id(shelf_data.id), path.clone(), tag.clone())
                        .await;

                    if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                        //[TODO] Sync Notification
                    }
                    match result {
                        Ok(_) => ReturnCode::Success,
                        Err(e) => e,
                    }
                }
                ShelfType::Remote => {
                    //[/] Request can be relayed, it is already atomic
                    match shelf.shelf_owner {
                        ShelfOwner::Node(peer_id) => {
                            match network.send_request(peer_id, Request::from(req)).await {
                                Ok(res) => parse_code(res.metadata().unwrap().return_code),
                                Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                            }
                        }
                        ShelfOwner::Sync(_sync_id) => {
                            todo!();
                        }
                    }
                }
            };

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(StripTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<DetachTag> for RpcService {
    type Response = DetachTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DetachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let mut state_db = self.state_db.clone();
        let mut network = self.network.clone();
        let mut filesys = self.filesys.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (shelf_id, tag_id, workspace_id) = (
                uuid(&req.shelf_id)?,
                uuid(&req.tag_id)?,
                uuid(&req.workspace_id)?,
            );

            let workspace = state_db.get_workspace(workspace_id).await?;

            let shelf = workspace
                .shelves
                .get(&shelf_id)
                .ok_or(ReturnCode::ShelfNotFound)?;

            let tag = workspace.tags.get(&tag_id).ok_or(ReturnCode::TagNotFound)?;

            //[/] Business Logic
            let return_code = {
                match &shelf.shelf_type {
                    ShelfType::Local => {
                        let shelf_key = ShelfDirKey::Path(shelf.info.load().root.to_path_buf());

                        let path = std::path::absolute(PathBuf::from(&req.path))
                            .map_err(|_| ReturnCode::PathNotFound)?;

                        let result = if path.is_file() {
                            filesys.detach_tag(shelf_key, path, tag.clone()).await
                        } else {
                            filesys.detach_dtag(shelf_key, path, tag.clone()).await
                        };

                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok((true, true)) => {
                                let mut up_filter = (**shelf.filter_tags.load()).clone();
                                up_filter.0.remove(&tag_id);
                                shelf.filter_tags.store(Arc::new(up_filter));
                                ReturnCode::Success
                            }
                            Ok((false, true)) => ReturnCode::Success,
                            Ok((_, false)) => ReturnCode::NotTagged, // File not tagged
                            Err(e) => return Err(e.into()),
                        }
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match network.send_request(peer_id, Request::from(req)).await {
                                    Ok(res) => parse_code(res.metadata().unwrap().return_code), // [!] Cuckoo filter should be updated with a sync mechanism
                                    Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                                }
                            }
                            ShelfOwner::Sync(_sync_id) => {
                                todo!();
                            }
                        }
                    }
                }
            };

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(DetachTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<AttachTag> for RpcService {
    type Response = AttachTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AttachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let mut filesys = self.filesys.clone();
        let mut state_db = self.state_db.clone();
        let mut network = self.network.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (shelf_id, tag_id, workspace_id) = (
                uuid(&req.shelf_id)?,
                uuid(&req.tag_id)?,
                uuid(&req.workspace_id)?,
            );

            let workspace = state_db.get_workspace(workspace_id).await?;

            let shelf = workspace
                .shelves
                .get(&shelf_id)
                .ok_or(ReturnCode::ShelfNotFound)?;

            let tag = workspace.tags.get(&tag_id).ok_or(ReturnCode::TagNotFound)?;

            //[/] Business Logic
            let return_code = {
                match &shelf.shelf_type {
                    ShelfType::Local => {
                        let shelf_data = filesys
                            .get_or_init_shelf(ShelfDirKey::Path(
                                shelf.info.load().root.to_path_buf(),
                            ))
                            .await
                            .unwrap();

                        let path = std::path::absolute(PathBuf::from(&req.path))
                            .map_err(|_| ReturnCode::PathNotFound)?;

                        let result = if path.is_file() {
                            filesys
                                .attach_tag(ShelfDirKey::Id(shelf_data.id), path, tag.clone())
                                .await
                        } else if path.is_dir() {
                            filesys
                                .attach_dtag(ShelfDirKey::Id(shelf_data.id), path, tag.clone())
                                .await
                        } else {
                            return Err(ReturnCode::PathNotDir.into());
                        };
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok((true, true)) => {
                                let mut up_filter = (**shelf.filter_tags.load()).clone();
                                up_filter.0.insert(&tag_id);
                                shelf.filter_tags.store(Arc::new(up_filter));
                                ReturnCode::Success
                            } // Success
                            Ok((false, true)) => ReturnCode::Success, // File not tagged
                            Ok((_, false)) => ReturnCode::TagAlreadyAttached,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match network.send_request(peer_id, Request::from(req)).await {
                                    Ok(res) => parse_code(res.metadata().unwrap().return_code), // [!] Cuckoo filter should be updated with a sync mechanism
                                    Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                                }
                            }
                            ShelfOwner::Sync(_sync_id) => {
                                todo!();
                            }
                        }
                    }
                }
            };

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(AttachTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<RemoveShelf> for RpcService {
    type Response = RemoveShelfResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveShelf) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let mut state_db = self.state_db.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (shelf_id, workspace_id) = (uuid(&req.shelf_id)?, uuid(&req.shelf_id)?);

            state_db
                .workspace(workspace_id)
                .unassign_shelf(shelf_id)
                .await?;

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data,
            };
            Ok(RemoveShelfResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditShelf> for RpcService {
    type Response = EditShelfResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditShelf) -> Self::Future {
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let mut state_db = self.state_db.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let (shelf_id, workspace_id) = (uuid(&req.shelf_id)?, uuid(&req.workspace_id)?);

            state_db
                .workspace(workspace_id)
                .edit_shelf_info(shelf_id, req.name, req.description)
                .await?;

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data,
            };

            Ok(EditShelfResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<AddShelf> for RpcService {
    type Response = AddShelfResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AddShelf) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let daemon_info = self.daemon_info.clone();
        let mut network = self.network.clone();

        Box::pin(async move {
            let error_data: Option<ErrorData> = None;
            let mut shelf_id: Option<ShelfId> = None;

            let workspace_id = uuid(&req.workspace_id)?;

            let peer_id = parse_peer_id(&req.peer_id)?;

            let path = std::path::absolute(PathBuf::from(&req.path))
                .map_err(|_| ReturnCode::PathNotFound)?;

            //[/] Business Logic
            let return_code = {
                if peer_id != *daemon_info.id {
                    match network.send_request(peer_id, Request::from(req)).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                    }
                } else {
                    match state_db
                        .workspace(workspace_id)
                        .assign_shelf(path, peer_id, false, req.description, req.name)
                        .await
                    {
                        Ok(id) => {
                            shelf_id = Some(id);
                            ReturnCode::Success
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            };

            let encoded_shelf_id = shelf_id.map(|shelf_id| shelf_id.as_bytes().to_vec());

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(AddShelfResponse {
                shelf_id: encoded_shelf_id,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<DeleteWorkspace> for RpcService {
    type Response = DeleteWorkspaceResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteWorkspace) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_id = uuid(&req.workspace_id)?;
            state_db.remove_workspace(workspace_id).await?;

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(DeleteWorkspaceResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditTag> for RpcService {
    type Response = EditTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditTag) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let (tag_id, workspace_id) = (uuid(&req.tag_id)?, uuid(&req.workspace_id)?);

            let workspace = state_db.get_workspace(workspace_id).await?;

            let tag = workspace.tags.get(&tag_id).ok_or(ReturnCode::TagNotFound)?;

            let parent = {
                if let Some(id) = req.parent_id.clone() {
                    let p_id = uuid(&id)?;
                    let parent_tag = workspace
                        .tags
                        .get(&p_id)
                        .ok_or(ReturnCode::ParentNotFound)?;
                    Some(parent_tag.clone())
                } else {
                    None
                }
            };

            //[/] Tag Name Validation
            if req.name.clone().is_empty() {
                return Err(ReturnCode::TagNameEmpty.into());
            }

            let return_code = {
                tag.rcu(|t| {
                    let mut u_t = (**t).clone();
                    u_t.name = req.name.clone();
                    u_t.priority = req.priority;
                    u_t.parent = parent.clone();
                    u_t
                });
                ReturnCode::Success
            };

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data: None,
            };
            Ok(EditTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditWorkspace> for RpcService {
    type Response = EditWorkspaceResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditWorkspace) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_id = uuid(&req.workspace_id)?;

            if req.name.clone().is_empty() {
                return Err(ReturnCode::WorkspaceNameEmpty.into());
            }

            state_db
                .workspace(workspace_id)
                .edit_workspace_info(req.name, req.description)
                .await?;

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(EditWorkspaceResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<GetShelves> for RpcService {
    type Response = GetShelvesResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelves) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_id = uuid(&req.workspace_id)?;
            let workspace = state_db.get_workspace(workspace_id).await?;

            let mut shelves = Vec::new();
            // [!] Can be changed with iter mut + impl Into<rpc::Shelf> for shelf
            for (id, shelf) in workspace.shelves.iter() {
                let owner_data = match &shelf.shelf_owner {
                    ShelfOwner::Node(node_id) => {
                        ebi_proto::rpc::shelf::Owner::NodeId(node_id.as_bytes().to_vec())
                    }
                    ShelfOwner::Sync(sync_id) => {
                        ebi_proto::rpc::shelf::Owner::SyncId(sync_id.as_bytes().to_vec())
                    }
                };

                shelves.push(Shelf {
                    shelf_id: id.as_bytes().to_vec(),
                    owner: Some(owner_data),
                    name: shelf.info.load().name.get(),
                    description: shelf.info.load().description.get(),
                    path: shelf.info.load().root.get().to_string_lossy().into_owned(),
                });
            }
            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(GetShelvesResponse {
                shelves,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<GetWorkspaces> for RpcService {
    type Response = GetWorkspacesResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspaces) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_ls = state_db.get_workspaces().await.unwrap();

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(GetWorkspacesResponse {
                workspaces: workspace_ls,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<CreateWorkspace> for RpcService {
    type Response = CreateWorkspaceResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let id = state_db
                .create_workspace(req.name, req.description)
                .await
                .unwrap();

            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(CreateWorkspaceResponse {
                workspace_id: id.as_bytes().to_vec(),
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<CreateTag> for RpcService {
    type Response = CreateTagResponse;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let mut state_db = self.state_db.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let workspace_id = uuid(&req.workspace_id)?;

            let parent_id = match req.parent_id {
                Some(parent_id) => Some(uuid(&parent_id)?),
                None => None,
            };

            let tag_id = state_db
                .workspace(workspace_id)
                .create_tag(req.priority, req.name, parent_id)
                .await?;

            // If the tag was created successfully, return the response with the tag ID
            let metadata = Status {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32, // Success
                error_data: None,
            };
            Ok(CreateTagResponse {
                tag_id: Some(tag_id.into_bytes().to_vec()),
                metadata: Some(metadata),
            })
        })
    }
}
