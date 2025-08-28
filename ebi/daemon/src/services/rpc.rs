use crate::services::peer::PeerService;
use crate::services::query::QueryService;
use crate::services::state::{
    AssignShelf, GetWorkspace, RemoveWorkspace, UnassignShelf,
    StateService,
};
use crate::sharedref::{ImmutRef, Ref, StatefulRef};
use crate::shelf::{ShelfId, ShelfOwner, ShelfType, UpdateErr};
use crate::stateful::StatefulUpdate;
use crate::workspace::Workspace;
use arc_swap::ArcSwap;
use bincode::serde::encode_to_vec;
use ebi_proto::rpc::*;
use iroh::NodeId;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{watch::{Receiver, Sender}, RwLock};
use tokio::task::JoinHandle;
use tower::Service;
use tracing::{Level, span};

use uuid::Uuid;

pub type RequestId = Uuid;

//[!] Potentially, we could have a validation
macro_rules! return_error {
    ($return_code:path, $response:ident, $request_uuid:expr, $error_data:ident) => {
        let return_code = $return_code;
        let metadata = ResponseMetadata {
            request_uuid: $request_uuid,
            return_code: return_code as u32,
            $error_data,
        };
        let mut res = $response::default();
        res.metadata = Some(metadata);
        return Ok(res);
    };
}

pub async fn try_get_workspace(
    rawid: &[u8],
    srv: &mut StateService,
) -> Result<Arc<StatefulRef<Workspace>>, ReturnCode> {
    let id = uuid(&rawid.to_owned()).map_err(|_| ReturnCode::ParseError)?;
    srv.call(GetWorkspace { id }).await
}

pub fn parse_peer_id(bytes: &[u8]) -> Result<NodeId, ()> {
    let bytes: &[u8; 32] = bytes.try_into().map_err(|_| ())?;
    NodeId::from_bytes(bytes).map_err(|_| ())
}

#[derive(Clone, Debug)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>,
    pub peer_srv: PeerService,
    pub state_srv: StateService,
    pub query_srv: QueryService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,

    // [!] This should be Mutexes. reason about read-write ratio
    pub responses: Arc<RwLock<HashMap<RequestId, Response>>>,
    pub notify_queue: Arc<RwLock<VecDeque<Notification>>>,
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

#[derive(Debug)]
pub struct DaemonInfo {
    pub id: NodeId,
    pub name: RwLock<String>,
}

impl DaemonInfo {
    pub fn new(id: NodeId, name: String) -> Self {
        DaemonInfo {
            id,
            name: RwLock::new(name),
        }
    }
}

#[derive(Debug)]
pub enum UuidErr {
    SizeMismatch,
}

pub fn uuid(bytes: &Vec<u8>) -> Result<Uuid, UuidErr> {
    let bytes: Result<[u8; 16], _> = bytes.clone().try_into();
    if let Ok(bytes) = bytes {
        Ok(Uuid::from_bytes(bytes))
    } else {
        Err(UuidErr::SizeMismatch)
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
            responses.write().await.insert(uuid, res);
            broadcast.send(uuid).map_err(|_| ())?;
            Ok(())
        })
    }
}

impl Service<ClientQuery> for RpcService {
    type Response = ClientQueryResponse;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut query_srv = self.query_srv.clone();
        Box::pin(async move {
            let Some(metadata) = req.clone().metadata else {
                return Err(ReturnCode::MalformedRequest);
            };

            match query_srv.call(req).await {
                Ok((token, packets)) => Ok(ClientQueryResponse {
                    token: token.as_bytes().to_vec(),
                    packets,
                    metadata: Some(ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: ReturnCode::Success as u32,
                        error_data: None,
                    }),
                }),
                Err(ret_code) => Ok(ClientQueryResponse {
                    token: vec![0],
                    packets: 0,
                    metadata: Some(ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: ret_code as u32,
                        error_data: None,
                    }),
                }),
            }
        })
    }
}

impl Service<PeerQuery> for RpcService {
    type Response = PeerQueryResponse;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: PeerQuery) -> Self::Future {
        let mut query_srv = self.query_srv.clone();
        Box::pin(async move {
            let config = bincode::config::standard(); // [TODO] this should be set globally
            match query_srv.call(req).await {
                Ok((files, errors)) => {
                    if let Ok(files) =
                        encode_to_vec(&files, config).map_err(|_| ReturnCode::ParseError)
                    {
                        Ok(PeerQueryResponse {
                            files,
                            metadata: Some(ResponseMetadata {
                                request_uuid: Uuid::now_v7().into(),
                                return_code: ReturnCode::Success as u32, // [?] Should this always be success ??
                                error_data: Some(ErrorData { error_data: errors }),
                            }),
                        })
                    } else {
                        Ok(PeerQueryResponse {
                            files: Vec::<u8>::new(),
                            metadata: Some(ResponseMetadata {
                                request_uuid: Uuid::now_v7().into(),
                                return_code: ReturnCode::PeerServiceError as u32, // [!] Encode Error
                                error_data: Some(ErrorData { error_data: errors }),
                            }),
                        })
                    }
                }
                Err(ret_code) => Ok(PeerQueryResponse {
                    files: Vec::<u8>::new(),
                    metadata: Some(ResponseMetadata {
                        request_uuid: Uuid::now_v7().into(),
                        return_code: ret_code as u32,
                        error_data: None,
                    }),
                }),
            }
        })
    }
}

impl Service<DeleteTag> for RpcService {
    type Response = DeleteTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut state_srv = self.state_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = workspace_ref.load();

            let Ok(tag_id) = uuid(&req.tag_id) else {
                return_error!(
                    ReturnCode::ParseError,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag_ref) = workspace.tags.get(&tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            for (_id, shelf) in workspace.shelves.iter() {
                match &shelf.shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let _ = shelf_data.strip(PathBuf::from(""), tag_ref);

                        //[?] Are (remote) Sync'd shelves also in shelves ??
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                    }
                    ShelfType::Remote => {
                        //[TODO] Remote Request
                        //[?] For all Nodes with Write-Permissions, relay ??
                        //[!] Request must be atomic, relaying may not be sufficient
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Handle the responses
                        // think of when to remove RwLocks
                    }
                }
            }

            workspace_ref.stateful_rcu(|w| {
                let (u_m, u_s) = w.tags.s_remove(&tag_id);
                let u_w = Workspace {
                    tags: u_m,
                    info: ArcSwap::new(w.info.load_full()),
                    shelves: w.shelves.clone(),
                    lookup: w.lookup.clone()
                };
                (u_w, u_s)
            }).await;

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Tag.into(),
                    id: tag_id.into_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: StripTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut state_srv = self.state_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let (Ok(shelf_id), Ok(tag_id)) = (uuid(&req.shelf_id), uuid(&req.tag_id)) else {
                return_error!(
                    ReturnCode::ParseError,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = workspace_ref.load();

            let Some(shelf) = workspace.shelves.get(&shelf_id) else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag) = workspace.tags.get(&tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };


            let return_code = match &shelf.shelf_type {
                ShelfType::Local(shelf_data) => {
                    let result = shelf_data.strip(PathBuf::from(&req.path), tag);

                    if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                        //[TODO] Sync Notification
                    }
                    match result {
                        Ok(_) => ReturnCode::Success,
                        Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // Path not a Directory
                        Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not Found
                        Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // "Nothing ever happens" -Chudda
                    }
                }
                ShelfType::Remote => {
                    //[/] Request can be relayed, it is already atomic
                    match shelf.shelf_owner {
                        ShelfOwner::Node(peer_id) => {
                            match peer_srv.call((peer_id, Request::from(req))).await {
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

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.into_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DetachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut state_srv = self.state_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let (Ok(shelf_id), Ok(tag_id)) = (uuid(&req.shelf_id), uuid(&req.tag_id)) else {
                return_error!(
                    ReturnCode::ParseError,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = workspace_ref.load();

            let Some(shelf) = workspace.shelves.get(&shelf_id) else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag) = workspace.tags.get(&tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };


            //[/] Business Logic
            let return_code = {
                match &shelf.shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let path = PathBuf::from(&req.path);
                        let result = if path.is_file() {
                            shelf_data.detach(path, tag)
                        } else {
                            shelf_data.detach_dtag(path, tag)
                        };

                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok((true, true)) => {
                                let mut up_filter = (**shelf.filter_tags.load()).clone();
                                up_filter.remove(&tag_id);
                                shelf.filter_tags.store(Arc::new(up_filter));
                                ReturnCode::Success
                            }
                            Ok((false, true)) => ReturnCode::Success,
                            Ok((_, false)) => ReturnCode::NotTagged, // File not tagged
                            Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not found
                            Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // File not found
                            Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // "Nothing ever happens" -Chudda
                        }
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match peer_srv.call((peer_id, Request::from(req))).await {
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

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.into_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AttachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut state_srv = self.state_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let (Ok(shelf_id), Ok(tag_id)) = (uuid(&req.shelf_id), uuid(&req.tag_id)) else {
                return_error!(
                    ReturnCode::ParseError,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = workspace_ref.load();

            let Some(shelf) = workspace.shelves.get(&shelf_id) else {

                return_error!(
                    ReturnCode::ShelfNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag) = workspace.tags.get(&tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            //[/] Business Logic
            let return_code = {
                match &shelf.shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let path = PathBuf::from(&req.path);
                        let result = if path.is_file() {
                            shelf_data.attach(path, tag)
                        } else {
                            shelf_data.attach_dtag(path, tag)
                        };
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok((true, true)) => {
                                let mut up_filter = (**shelf.filter_tags.load()).clone();
                                up_filter.insert(&tag_id);
                                shelf.filter_tags.store(Arc::new(up_filter));
                                ReturnCode::Success
                            } // Success
                            Ok((false, true)) => ReturnCode::Success, // File not tagged
                            Ok((_, false)) => ReturnCode::TagAlreadyAttached,
                            Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not found
                            Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // File not found
                            Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // "Nothing ever happens" -Chudda
                        }
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match peer_srv.call((peer_id, Request::from(req))).await {
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

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.into_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveShelf) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut state_srv = self.state_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(shelf_id) = uuid(&req.shelf_id) else {
                return_error!(
                    ReturnCode::ParseError,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = workspace_ref.load();

            let Some(shelf) = workspace.shelves.get(&shelf_id) else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };


            //[/] Business Logic
            let return_code = {
                match &shelf.shelf_type {
                    ShelfType::Local(_) => {
                        let _result = state_srv
                            .call(UnassignShelf {
                                shelf_id,
                                workspace_id: workspace_ref.id,
                            })
                            .await;
                        notify_queue.write().await.push_back({
                            Notification::Operation(Operation {
                                target: ActionTarget::Shelf.into(),
                                id: shelf_id.as_bytes().to_vec(),
                                action: ActionType::Delete.into(),
                            })
                        });
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        ReturnCode::Success
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match peer_srv.call((peer_id, Request::from(req))).await {
                                    Ok(res) => parse_code(res.metadata().unwrap().return_code),
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

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: workspace_ref.id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditShelf) -> Self::Future {
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut state_srv = self.state_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(shelf_id) = uuid(&req.shelf_id) else {
                return_error!(
                    ReturnCode::ParseError,
                    EditShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = workspace_ref.load();

            let Some(shelf) = workspace.shelves.get(&shelf_id) else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    EditShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                match shelf.shelf_type {
                    ShelfType::Local(_) => {
                        workspace_ref.stateful_rcu(|w| {
                            let mut s = (***shelf).clone();
                            s.edit_info(Some(req.name.clone()), Some(req.description.clone()).clone());
                            let (u_m, u_s) = w.shelves.s_insert((shelf.id, ImmutRef::new_ref(s)));
                            let u_w = Workspace {
                                shelves: u_m,
                                tags: w.tags.clone(),
                                lookup: w.lookup.clone(),
                                info: ArcSwap::new(w.info.load_full())
                            };
                            (u_w, u_s)
                        }).await;
                        if let ShelfOwner::Sync(_sync_id) = shelf.shelf_owner {
                            //[TODO] Sync Notification
                        }
                        ReturnCode::Success
                    }
                    ShelfType::Remote => {
                        match shelf.shelf_owner {
                            ShelfOwner::Node(peer_id) => {
                                match peer_srv.call((peer_id, Request::from(req))).await {
                                    Ok(res) => parse_code(res.metadata().unwrap().return_code),
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

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: shelf_id.into_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AddShelf) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let daemon_info = self.daemon_info.clone();
        let mut peer_srv = self.peer_srv.clone();

        Box::pin(async move {
            let error_data: Option<ErrorData> = None;
            let mut shelf_id: Option<ShelfId> = None;

            let Ok(workspace_id) = uuid(&req.workspace_id) else {
                return_error!(
                    ReturnCode::ParseError,
                    AddShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(peer_id) = parse_peer_id(&req.peer_id) else {
                return_error!(
                    ReturnCode::PeerNotFound, //[!] Change to UuidParseErr, peer validation is done
                    //in StateService
                    AddShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                if peer_id != daemon_info.id {
                    match peer_srv.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                    }
                } else {
                    match state_srv
                        .call(AssignShelf {
                            path: req.path.into(),
                            node_id: peer_id,
                            remote: false,
                            description: req.description,
                            name: req.name,
                            workspace_id,
                        })
                        .await
                    {
                        Ok(id) => {
                            shelf_id = Some(id);
                            ReturnCode::Success
                        }
                        Err(e) => e,
                    }
                }
            };

            let encoded_shelf_id = shelf_id.map(|shelf_id| shelf_id.as_bytes().to_vec());

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: workspace_id.into_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }
            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteWorkspace) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_id) = uuid(&req.workspace_id) else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(()) = state_srv.call(RemoveWorkspace { workspace_id }).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: workspace_id.into_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditTag) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = workspace_ref.load();

            let Ok(tag_id) = uuid(&req.tag_id) else {
                return_error!(
                    ReturnCode::ParseError,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag) = workspace.tags.get(&tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let parent = {
                if let Some(id) = req.parent_id.clone() {
                    let Ok(parent_id) = uuid(&id) else {
                        return_error!(
                            ReturnCode::ParseError,
                            EditTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    };

                    let Some(parent_tag) = workspace.tags.get(&parent_id) else {
                        return_error!(
                            ReturnCode::ParentNotFound,
                            EditTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    }; 
                    Some(parent_tag)

                } else {
                    None
                }
            };

            //[/] Tag Name Validation
            if req.name.clone().is_empty() {
                return_error!(
                    ReturnCode::TagNameEmpty,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            }

            //[/] Business Logic
            let return_code = {
                todo!();
                /*
                tag.rcu(|t| {
                    let mut u_t = (**t).clone();
                    u_t.name = req.name.clone();
                    u_t.priority = req.priority;
                    u_t.parent = parent.clone();
                    u_t
                });
                */
                ReturnCode::Success
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Tag.into(),
                        id: req.tag_id,
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditWorkspace) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            if req.name.clone().is_empty() {
                let return_code = ReturnCode::WorkspaceNameEmpty;
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditWorkspaceResponse {
                    metadata: Some(metadata),
                });
            }

            //[/] Business Logic
            let return_code = {
                let mut ws_info = (**workspace.load().info.load()).clone();
                ws_info.name = req.name;
                ws_info.description = req.description;
                workspace.load().info.store(Arc::new(ws_info));
                ReturnCode::Success
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelves) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    GetShelvesResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let mut shelves = Vec::new();
            let workspace = workspace.load();
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
                    name: shelf.info.name.clone(),
                    description: shelf.info.description.clone(),
                    path: shelf.info.root_path.to_string_lossy().into_owned(),
                });
            }
            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspaces) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_ls = state_srv
                .call(crate::services::state::GetWorkspaces {})
                .await
                .unwrap();

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let id = state_srv
                .call(crate::services::state::CreateWorkspace {
                    name: req.name,
                    description: req.description,
                })
                .await
                .unwrap();

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: id.as_bytes().to_vec(),
                    action: ActionType::Create.into(),
                })
            });

            let metadata = ResponseMetadata {
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
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    CreateTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = workspace_ref.load();

            if workspace.lookup.contains_key(&req.name) {
                return_error!(
                    ReturnCode::TagNameDuplicate,
                    CreateTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

           let parent = {
               if let Some(parent_id) = req.parent_id {

                   let Ok(parent_id) = uuid(&parent_id) else {
                        return_error!(
                            ReturnCode::ParseError,
                            CreateTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    };

                    let Some(parent_tag) = workspace.tags.get(&parent_id) else {
                        return_error!(
                            ReturnCode::TagNotFound,
                            CreateTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    };

                    Some(parent_tag.clone())
               } else {
                   None
               }
           };

            let c_tag = state_srv
                .call(crate::services::state::CreateTag {
                    priority: req.priority,
                    parent,
                    name: req.name.clone(),
                }).await;

            let Ok(tag) = c_tag else {
                let ret_code = c_tag.unwrap_err();
                return_error!(
                    ret_code,
                    CreateTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            workspace_ref.stateful_rcu(|w| {
                let (u_l, _) = w.lookup.s_insert((req.name.clone(), tag.id.clone()));
                let (u_t, u_s) = w.tags.s_insert((tag.id.clone(), tag.clone()));
                let u_w = Workspace {
                    tags: u_t,
                    info: ArcSwap::new(w.info.load_full()),
                    shelves: w.shelves.clone(),
                    lookup: u_l,
                };
                (u_w, u_s)
            }).await;

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Tag.into(),
                    id: tag.id.into_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });
            // If the tag was created successfully, return the response with the tag ID
            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32, // Success
                error_data: None,
            };
            Ok(CreateTagResponse {
                tag_id: Some(tag.id.into_bytes().to_vec()),
                metadata: Some(metadata),
            })
        })
    }
}
