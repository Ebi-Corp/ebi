use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, QueryErr, RetrieveErr, RetrieveService};
use crate::services::cache::CacheService;
use crate::services::peer::{PeerError, PeerService};
use crate::services::rpc::{parse_peer_id, try_get_workspace, uuid, DaemonInfo};
use crate::services::workspace::WorkspaceService;
use crate::shelf::node;
use crate::shelf::shelf::{ShelfOwner, ShelfType};
use crate::tag::TagId;
use crate::workspace::WorkspaceId;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use chrono::{DateTime, Utc};
use ebi_proto::rpc::{
    parse_code, ClientQuery, ClientQueryData, ClientQueryResponse, Data, DataCode, Empty, ErrorData, File, FileMetadata, PeerQuery, PeerQueryResponse, ReqMetadata, Request, RequestMetadata, ResMetadata, Response, ReturnCode, UnixMetadata, WindowsMetadata
};
use iroh::NodeId;
use uuid::Uuid;
use core::error;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs::Metadata;
use std::{result, vec};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::{JoinSet, JoinHandle};
use tower::Service;

type TaskID = u64;

#[derive(Clone)]
struct QueryService {
    retrieve_serv: Retrieve,
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
    daemon_info: Arc<DaemonInfo>
}

#[derive(Clone)]
struct Retrieve {
    cache: CacheService,
}

impl RetrieveService for Retrieve {
    async fn get_files(
        &self,
        _workspace_id: WorkspaceId,
        _tag_id: TagId,
    ) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        todo!();
    }

    async fn get_all(&self) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        todo!();
    }
}

impl Service<PeerQuery> for QueryService {
    type Response = PeerQueryResponse;
    type Error = (); //[!] Query Request Errors
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        let node_id = self.daemon_info.id.clone();
        Box::pin(async move {});
    }
}

impl Service<ClientQuery> for QueryService {
    type Response = ();
    type Error = (); //[!] Query Request Errors
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut retrieve_srv = self.retrieve_serv.clone();
        let node_id = self.daemon_info.id.clone();
        Box::pin(async move {
            let query_str = req.query;
            
            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };
            let workspace_id = workspace.read().await.id.clone();

            let Some(metadata) = req.metadata
            else {
                return Err(ReturnCode::MalformedRequest);
            };

            let client_node = parse_peer_id(&metadata.source_id).unwrap();

            let file_order = FileOrder::try_from(req.file_ord.unwrap().order_by)?;
            let mut query = Query::new(&query_str, file_order, req.file_ord.unwrap().ascending)
                .map_err(|_| ReturnCode::InternalStateError)?;

            let shelves = &workspace.read().await.shelves;
            let mut nodes = HashSet::<NodeId>::new();
            for (_, shelf) in shelves {
                let shelf_r = shelf.read().await;
                match shelf_r.shelf_type {
                    ShelfType::Local(_) => {} //[?] Perform local queries after sending remote requests ?? 
                    ShelfType::Remote => {
                        if query.may_hold(shelf_r.get_tags()).await {
                            match shelf.read().await.shelf_owner {
                                ShelfOwner::Node(node) => {
                                    nodes.insert(node);
                                }
                                ShelfOwner::Sync(_) => {} // [!] Implement Sync logic
                            }
                        }
                    }
                }
            }
            
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let serialized_query =
                encode_to_vec(&query, config).map_err(|_| ReturnCode::ParseError)?;

            let peer_query;
            {
                let metadata = metadata.clone();
                peer_query = Request::from(PeerQuery {
                    query: serialized_query,
                    workspace_id: req.workspace_id,
                    metadata: Some(RequestMetadata {
                        request_uuid: metadata.request_uuid,
                        source_id: node_id.as_bytes().to_vec(),
                        relayed: true
                    }),
                });
            }
            //[/] Return ClientQueryResponse in RpcService before asynchronously calling QueryService 

            let mut futures = JoinSet::<Result<Response, PeerError>>::new();
            for node in nodes {
                let mut peer_srv = peer_srv.clone();
                let peer_req = peer_query.clone();
                futures.spawn(async move {
                    peer_srv.call((node, peer_req)).await
                });
            }

            

            fn encode_timestamp(opt_dt: Option<DateTime<Utc>>) -> Option<i64> {
                opt_dt.map_or_else(|| None, |dt| Some(dt.timestamp()))
            }

            if req.atomic {
                let mut files = Vec::<BTreeSet<OrderedFileSummary>>::new();
                let mut errors = Vec::<String>::new();

                //Local Query
                let local_res = query.evaluate(workspace_id, retrieve_srv).await;
                match local_res {
                    Ok(res) => files.push(res),
                    Err(err) => {
                        let err =  match err {
                            QueryErr::SyntaxError => {
                                format!("[{:?}] Query Parse Error ", node_id.as_bytes().to_vec())
                            },
                            QueryErr::ParseError => {
                                format!("[{:?}] Tag Parse Error", node_id.as_bytes().to_vec())
                            },
                            QueryErr::RuntimeError(ret_err) => {
                                format!("[{:?}] Runtime Error: {:?}", node_id.as_bytes().to_vec(), ret_err)
                            }
                        };
                        errors.push(err)
                    },
                }

                while let Some(result) = futures.join_next().await {
                    if result.is_err(){
                        errors.push(format!("Join error: {:?}", result.unwrap_err()));
                        continue;
                    } else {
                        let result = result.unwrap();
                        if result.is_err() {
                            errors.push(format!("Peer error: {:?}", result.unwrap_err()));
                            continue;
                        } else {
                            let result = result.unwrap();
                            let res_metadata;
                            if let Some(meta) = result.metadata() {
                                res_metadata = meta;
                            } else {
                                errors.push("[?]: Malformed Response".to_string());
                                continue;
                            }
                            let node = res_metadata.source_id;
                            match result {
                                Response::PeerQueryResponse(res) => {                         
                                    match parse_code(res_metadata.return_code) {
                                        ReturnCode::Success => {
                                            //[?] Correct way to deserialize res.files into BTreeSet<OrderedFileSummary> ?? 
                                            match borrow_decode_from_slice::<BTreeSet<OrderedFileSummary>, _>(&res.files, config) {
                                                Ok((deserialized_files, _)) => {
                                                    files.push(deserialized_files);
                                                }
                                                Err(e) => {
                                                    errors.push(format!("[{:?}] Deserialization error: {:?}", node, e));
                                                }
                                            }
                                        }
                                        _ => {
                                            let error_str = match res_metadata.error_data {
                                                Some(data) => data.error_data.join("\n"),
                                                None => "Unknown error".to_string()
                                            };
                                            errors.push(format!("[{:?}] Query Error: {:?}", node, error_str));
                                        }
                                    }
                                }
                                res => {
                                    errors.push(format!("[{:?}]: Unexpected response type - {:?}", node, res));
                                }
                            }
                        }
                    }
                }

                //[?] Is the tournament-style Merge-Sort approach the most efficient method ?? 
                //[/] BTreeSets are not guaranteed to be the same size 
                //[TODO] Time & Space Complexity analysis
                let file_vec: Vec<OrderedFileSummary>;
                if files.is_empty() {
                    file_vec = Vec::new();
                } else {
                    while files.len() > 1 {
                        let mut next_round = Vec::with_capacity((files.len() + 1) / 2);
                        let mut chunks = files.chunks_exact(2);
                        
                        if let Some(remainder) = chunks.remainder().first() {
                            next_round.push(remainder.clone());
                        }

                        for chunk in chunks.by_ref() {  //[TODO] Parallelise merge 
                            let a = &chunk[0];
                            let b = &chunk[1];
                            
                            // Merging the smaller set into the larger one is more efficient
                            let merged_tree = if a.len() <= b.len() {
                                b.union(a).cloned().collect()
                            } else {
                                a.union(b).cloned().collect()
                            };
                            next_round.push(merged_tree);
                        }
                        
                        files = next_round;
                    }
                    file_vec = Vec::from_iter(files[0].clone().into_iter());
                }

                let metadata = metadata.clone();
                let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                    token: metadata.request_uuid,
                    return_code: ReturnCode::Success as u32, //[?] Should this always be success ?? 
                    files: file_vec.iter().map(|f| File { 
                        path: f.file_summary.path.to_string_lossy().to_string(), 
                        metadata: Some(FileMetadata {
                            size: f.file_summary.metadata.size,
                            readonly: f.file_summary.metadata.readonly,
                            modified: encode_timestamp(f.file_summary.metadata.modified),
                            accessed: encode_timestamp(f.file_summary.metadata.accessed),
                            created: encode_timestamp(f.file_summary.metadata.created),
                            os_metadata: {
                                let f_meta = f.file_summary.metadata.clone();
                                let os_metadata = match (f_meta.unix, f_meta.windows) {
                                    (Some(unix), None) => {
                                        ebi_proto::rpc::file_metadata::OsMetadata::Unix( UnixMetadata {
                                            permissions: unix.permissions, 
                                            uid: unix.uid, 
                                            gid: unix.gid 
                                        })
                                    }
                                    (None, Some(windows)) => {
                                        ebi_proto::rpc::file_metadata::OsMetadata::Windows( WindowsMetadata {
                                            attributes: windows.attributes
                                        })}
                                    _ => {
                                        ebi_proto::rpc::file_metadata::OsMetadata::Error( Empty {} )
                                    }
                                };
                                Some(os_metadata)
                            }
                        }
                    )}).collect(),
                    error_data: Some(ErrorData {
                        error_data: errors
                    })
                }))).await;
            } else {
                //[TODO] Add query-specific errors 

                //Local query
                let local_res = query.evaluate(workspace_id, retrieve_srv).await;
                match local_res {
                    Ok(res) => {
                        let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                            token: metadata.clone().request_uuid,
                            return_code: ReturnCode::Success as u32,
                            files: res.iter().map(|f| File { path: f.file_summary.path.to_string_lossy().to_string(), 
                                metadata: Some(FileMetadata {
                                    size: f.file_summary.metadata.size,
                                    readonly: f.file_summary.metadata.readonly,
                                    modified: encode_timestamp(f.file_summary.metadata.modified),
                                    accessed: encode_timestamp(f.file_summary.metadata.accessed),
                                    created: encode_timestamp(f.file_summary.metadata.created),
                                    os_metadata: {
                                        let f_meta = f.file_summary.metadata.clone();
                                        let os_metadata = match (f_meta.unix, f_meta.windows) {
                                            (Some(unix), None) => {
                                                ebi_proto::rpc::file_metadata::OsMetadata::Unix( UnixMetadata {
                                                    permissions: unix.permissions, 
                                                    uid: unix.uid, 
                                                    gid: unix.gid 
                                                })
                                            }
                                            (None, Some(windows)) => {
                                                ebi_proto::rpc::file_metadata::OsMetadata::Windows( WindowsMetadata {
                                                    attributes: windows.attributes
                                                })}
                                            _ => {
                                                ebi_proto::rpc::file_metadata::OsMetadata::Error( Empty {} )
                                            }
                                        };
                                        Some(os_metadata)
                                    }
                                }
                            )}).collect(),
                            error_data: None
                        }))).await;
                    },
                    Err(err) => {
                        let err =  match err {
                            QueryErr::SyntaxError => {
                                format!("[{:?}] Query Parse Error ", node_id.as_bytes().to_vec())
                            },
                            QueryErr::ParseError => {
                                format!("[{:?}] Tag Parse Error", node_id.as_bytes().to_vec())
                            },
                            QueryErr::RuntimeError(ret_err) => {
                                format!("[{:?}] Runtime Error: {:?}", node_id.as_bytes().to_vec(), ret_err)
                            }
                        };
                        let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                            token: metadata.clone().request_uuid,
                            return_code: ReturnCode::Success as u32,
                            files: Vec::new(),
                            error_data: Some(ErrorData { error_data: vec![err]})
                        }))).await;
                    },
                }

                while let Some(result) = futures.join_next().await {
                    let metadata = metadata.clone();
                    if result.is_err() {
                        let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                            token: metadata.request_uuid,
                            return_code: ReturnCode::PeerServiceError as u32, //[?] Is JoinError too low-level to return ?? 
                            files: Vec::new(),
                            error_data: Some(ErrorData { error_data: vec![format!("Join error: {:?}", result.unwrap_err())] })
                        }))).await;
                        continue;
                    } else {
                        let result = result.unwrap();
                        if result.is_err() {
                            let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                token: metadata.request_uuid,
                                return_code: ReturnCode::PeerServiceError as u32,
                                files: Vec::new(),
                                error_data: Some(ErrorData { error_data: vec![format!("Peer error: {:?}", result.unwrap_err())] })
                            }))).await;
                            continue;
                        } else{
                            let result = result.unwrap();
                            let res_metadata;
                            if let Some(meta) = result.metadata() {
                                res_metadata = meta;
                            } else {
                                let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                    token: metadata.request_uuid,
                                    return_code: ReturnCode::PeerServiceError as u32,
                                    files: Vec::new(),
                                    error_data: Some(ErrorData { error_data: vec!["[?]: Malformed Response".to_string()] })
                                }))).await;
                                continue;
                            }
                            let node = res_metadata.source_id;
                            match result {
                                Response::PeerQueryResponse(res) => {                         
                                    match parse_code(res_metadata.return_code) {
                                        ReturnCode::Success => {
                                            //[?] Correct way to deserialize res.files into BTreeSet<OrderedFileSummary> ?? 
                                            match borrow_decode_from_slice::<Vec<OrderedFileSummary>, _>(&res.files, config) {
                                                Ok((deserialized_files, _)) => {
                                                    let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                                        token: metadata.request_uuid,
                                                        return_code: ReturnCode::Success as u32,
                                                        files: deserialized_files.iter().map(|f| File { path: f.file_summary.path.to_string_lossy().to_string(), 
                                                            metadata: Some(FileMetadata {
                                                                size: f.file_summary.metadata.size,
                                                                readonly: f.file_summary.metadata.readonly,
                                                                modified: encode_timestamp(f.file_summary.metadata.modified),
                                                                accessed: encode_timestamp(f.file_summary.metadata.accessed),
                                                                created: encode_timestamp(f.file_summary.metadata.created),
                                                                os_metadata: {
                                                                    let f_meta = f.file_summary.metadata.clone();
                                                                    let os_metadata = match (f_meta.unix, f_meta.windows) {
                                                                        (Some(unix), None) => {
                                                                            ebi_proto::rpc::file_metadata::OsMetadata::Unix( UnixMetadata {
                                                                                permissions: unix.permissions, 
                                                                                uid: unix.uid, 
                                                                                gid: unix.gid 
                                                                            })
                                                                        }
                                                                        (None, Some(windows)) => {
                                                                            ebi_proto::rpc::file_metadata::OsMetadata::Windows( WindowsMetadata {
                                                                                attributes: windows.attributes
                                                                            })}
                                                                        _ => {
                                                                            ebi_proto::rpc::file_metadata::OsMetadata::Error( Empty {} )
                                                                        }
                                                                    };
                                                                    Some(os_metadata)
                                                                }
                                                            }
                                                        )}).collect(),
                                                        error_data: match res_metadata.error_data {
                                                            Some(err) => {
                                                                Some(err)
                                                            },
                                                            None => None
                                                        }
                                                    }))).await;
                                                    continue;
                                                }
                                                Err(e) => {
                                                    let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                                        token: metadata.request_uuid,
                                                        return_code: ReturnCode::PeerServiceError as u32,
                                                        files: Vec::new(),
                                                        error_data: Some(ErrorData { error_data: vec![format!("[{:?}] Deserialization error: {:?}", node, e)] })
                                                    }))).await;
                                                    continue;
                                                }
                                            }
                                        }
                                        _ => {
                                            let error_str = match res_metadata.error_data {
                                                Some(data) => data.error_data.join("\n"),
                                                None => "Unknown error".to_string()
                                            };
                                            let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                                token: metadata.request_uuid,
                                                return_code: ReturnCode::PeerServiceError as u32,
                                                files: Vec::new(),
                                                error_data: Some(ErrorData { error_data: vec![format!("[{:?}] Query Error: {:?}", node, error_str)] })
                                            }))).await;
                                            continue;
                                        }
                                    }
                                }
                                res => {
                                    let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                        token: metadata.request_uuid,
                                        return_code: ReturnCode::PeerServiceError as u32,
                                        files: Vec::new(),
                                        error_data: Some(ErrorData { error_data: vec![format!("[{:?}]: Unexpected response type - {:?}", node, res)] })
                                    }))).await;
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        });
        todo!();
    }
}
