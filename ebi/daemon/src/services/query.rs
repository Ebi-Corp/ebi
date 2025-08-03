use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, QueryErr, RetrieveErr, RetrieveService};
use crate::services::cache::{CacheService, RetrieveFiles};
use crate::services::peer::{PeerError, PeerService};
use crate::services::rpc::{DaemonInfo, parse_peer_id, try_get_workspace, uuid};
use crate::services::workspace::WorkspaceService;
use crate::shelf::node;
use crate::shelf::shelf::{ShelfOwner, ShelfType, ShelfId};
use crate::tag::TagId;
use crate::workspace::WorkspaceId;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use chrono::{DateTime, Utc};
use core::error;
use ebi_proto::rpc::{
    ClientQuery, ClientQueryData, ClientQueryResponse, Data, DataCode, Empty, ErrorData, File,
    FileMetadata, PeerQuery, PeerQueryResponse, ReqMetadata, Request, RequestMetadata, ResMetadata,
    Response, ResponseMetadata, ReturnCode, UnixMetadata, WindowsMetadata, parse_code,
};
use iroh::NodeId;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs::Metadata;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use std::{result, vec};
use tokio::task::{JoinHandle, JoinSet};
use tower::Service;
use uuid::Uuid;

type TaskID = u64;
pub type TokenId = Uuid;

#[derive(Clone)]
struct QueryService {
    retrieve_serv: Retrieve,
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
    daemon_info: Arc<DaemonInfo>,
}

#[derive(Clone)]
struct Retrieve {
    cache: CacheService,
    tag_shelves: HashMap<TagId, Vec<ShelfId>>,
    order: FileOrder
}

impl RetrieveService for Retrieve {
    async fn get_tagged(
        &self,
        workspace_id: WorkspaceId,
        tag_id: TagId,
    ) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        let mut cache = self.cache.clone();
        Ok(cache.call(RetrieveFiles::GetTagged(workspace_id, self.order.clone(), (&self.tag_shelves.get(&tag_id).unwrap()).to_vec(), tag_id)).await.unwrap())
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

    fn call(&mut self, req: PeerQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        let node_id = self.daemon_info.id.clone();
        Box::pin(async move {});
        todo!();
    }
}

impl Service<ClientQuery> for QueryService {
    type Response = (TokenId, u32);
    type Error = ebi_proto::rpc::ReturnCode; //[!] Query Request Errors
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

            let Some(metadata) = req.metadata else {
                return Err(ReturnCode::MalformedRequest);
            };

            let client_node = parse_peer_id(&metadata.source_id).unwrap();

            let file_order = FileOrder::try_from(req.file_ord.unwrap().order_by)?;
            let mut query = Query::new(&query_str, file_order, req.file_ord.unwrap().ascending)
                .map_err(|_| ReturnCode::InternalStateError)?;

            let shelves = &workspace.read().await.shelves;
            let query_tags = query.get_tags();
            let mut nodes = HashSet::<NodeId>::new();
            for (shelf_id, shelf) in shelves {
                let shelf_r = shelf.read().await;
                let filter = shelf_r.filter_tags.clone();
                let shelf_owner = shelf_r.shelf_owner.clone();
                drop(shelf_r);
                let filtered_tags: HashSet<TagId> = query_tags
                    .clone()
                    .into_iter()
                    .filter(|x| filter.contains(x))
                    .collect();
                match shelf_owner {
                    ShelfOwner::Node(node_owner) => {
                        if node_owner != node_id && query.may_hold(&filtered_tags) {
                            nodes.insert(node_owner);
                            filtered_tags.into_iter().for_each(|x| {
                                retrieve_srv
                                    .tag_shelves
                                    .entry(x)
                                    .or_default()
                                    .push(shelf_id.clone());
                            });
                        }
                    }
                    ShelfOwner::Sync(_) => {} // [TODO] Sync version
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
                        relayed: true,
                    }),
                });
            }
            let packets: u32 = if !req.atomic {
                (nodes.len() + 1).try_into().unwrap()
            } else {
                1
            };

            let token = Uuid::now_v7();
            let ser_token: Vec<u8> = token.into();

            //[/] Return ClientQueryResponse in RpcService before asynchronously calling QueryService

            let mut futures = JoinSet::<Result<Response, PeerError>>::new();
            let mut node_tasks = HashMap::<tokio::task::Id, NodeId>::new();
            for node in nodes {
                let mut peer_srv = peer_srv.clone();
                let peer_req = peer_query.clone();
                let id = futures
                    .spawn(async move { peer_srv.call((node, peer_req)).await })
                    .id();
                node_tasks.insert(id, node);
            }

            fn encode_timestamp(opt_dt: Option<DateTime<Utc>>) -> Option<i64> {
                opt_dt.map_or_else(|| None, |dt| Some(dt.timestamp()))
            }

            if req.atomic {
                tokio::spawn(async move {
                    let mut files = Vec::<BTreeSet<OrderedFileSummary>>::new();
                    let mut errors = Vec::<String>::new();

                    //Local Query
                    let local_res = query.evaluate(workspace_id, retrieve_srv).await;
                    match local_res {
                        Ok(res) => files.push(res),
                        Err(QueryErr::SyntaxError) => {
                            errors.push(format!(
                                "[{:?}] Query Parse Error ",
                                node_id.as_bytes().to_vec()
                            ));
                        }
                        Err(QueryErr::ParseError) => {
                            errors.push(format!(
                                "[{:?}] Tag Parse Error ",
                                node_id.as_bytes().to_vec()
                            ));
                        }
                        Err(QueryErr::RuntimeError(ret_err)) => {
                            errors.push(format!(
                                "[{:?}] Runtime Error: {:?}",
                                node_id.as_bytes().to_vec(),
                                ret_err
                            ));
                        }
                    }

                    while let Some(result) = futures.join_next_with_id().await {
                        match result {
                            Err(err) => errors.push(format!(
                                "[{:?}] Thread error: {:?}",
                                node_tasks.get(&err.id()).unwrap(),
                                err
                            )),
                            Ok((task_id, join_res)) => {
                                let node_id = node_tasks.get(&task_id).unwrap();
                                match join_res {
                                    Err(err) => errors
                                        .push(format!("[{:?}] Peer error: {:?}", node_id, err)),
                                    Ok(Response::PeerQueryResponse(peer_res)) => {
                                        let res_metadata = peer_res.metadata.unwrap();
                                        match parse_code(res_metadata.return_code) {
                                            ReturnCode::Success => {
                                                //[?] Correct way to deserialize res.files into BTreeSet<OrderedFileSummary> ??
                                                match borrow_decode_from_slice::<
                                                    BTreeSet<OrderedFileSummary>,
                                                    _,
                                                >(
                                                    &peer_res.files, config
                                                ) {
                                                    Ok((deserialized_files, _)) => {
                                                        files.push(deserialized_files);
                                                    }
                                                    Err(e) => {
                                                        errors.push(format!(
                                                            "[{:?}] Deserialization error: {:?}",
                                                            node_id, e
                                                        ));
                                                    }
                                                }
                                            }
                                            return_code => {
                                                let error_str = match res_metadata.error_data {
                                                    Some(data) => data.error_data.join("\n"),
                                                    None => "Unknown error".to_string(),
                                                };
                                                errors.push(format!(
                                                    "[{:?}] Query Error: {:?} Error data: {:?}",
                                                    node_id, return_code, error_str
                                                ));
                                            }
                                        }
                                    }
                                    Ok(_) => errors.push(format!(
                                        "[{:?}]: Unexpected response type - {:?}",
                                        node_id, join_res
                                    )),
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

                            for chunk in chunks.by_ref() {
                                //[TODO] Parallelise merge
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
                            token: ser_token,
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
                            metadata: Some(ResponseMetadata {
                                request_uuid: Uuid::now_v7().into(),
                                return_code: ReturnCode::Success as u32, // [?] Should this always be success ?? 
                                error_data: Some(ErrorData {
                                    error_data: errors
                                })
                            })
                        }))).await;
                });
            } else {
                tokio::spawn(async move {
                    //Local query
                    let local_res = query.evaluate(workspace_id, retrieve_srv).await;
                    match local_res {
                        Ok(res) => {
                            let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                    token: ser_token.clone(),
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
                                    metadata: Some(ResponseMetadata {
                                        request_uuid: Uuid::now_v7().into(),
                                        return_code: ReturnCode::Success as u32,
                                        error_data: None,
                                    })
                                }))).await;
                        }
                        Err(err) => {
                            let err = match err {
                                QueryErr::SyntaxError => {
                                    format!(
                                        "[{:?}] Query Parse Error ",
                                        node_id.as_bytes().to_vec()
                                    )
                                }
                                QueryErr::ParseError => {
                                    format!("[{:?}] Tag Parse Error", node_id.as_bytes().to_vec())
                                }
                                QueryErr::RuntimeError(ret_err) => {
                                    format!(
                                        "[{:?}] Runtime Error: {:?}",
                                        node_id.as_bytes().to_vec(),
                                        ret_err
                                    )
                                }
                            };
                            let _ = peer_srv
                                .call((
                                    client_node,
                                    Data::ClientQueryData(ClientQueryData {
                                        //[!] ClientQueryData is the only (used) Data-type RPC
                                        token: ser_token.clone(),
                                        files: Vec::new(),
                                        metadata: Some(ResponseMetadata {
                                            request_uuid: Uuid::now_v7().into(),
                                            return_code: ReturnCode::Success as u32,
                                            error_data: Some(ErrorData {
                                                error_data: vec![err],
                                            }),
                                        }),
                                    }),
                                ))
                                .await;
                        }
                    }

                    while let Some(join_result) = futures.join_next_with_id().await {
                        let ser_token = ser_token.clone();
                        match join_result {
                            Err(join_err) => {
                                let _ = peer_srv
                                    .call((
                                        client_node,
                                        Data::ClientQueryData(ClientQueryData {
                                            //[!] ClientQueryData is the only (used) Data-type RPC
                                            token: ser_token,
                                            files: Vec::new(),
                                            metadata: Some(ResponseMetadata {
                                                request_uuid: Uuid::now_v7().into(),
                                                return_code: ReturnCode::PeerServiceError as u32,
                                                error_data: Some(ErrorData {
                                                    error_data: vec![format!(
                                                        "[{:?}] Join error: {:?}",
                                                        node_tasks.get(&join_err.id()).unwrap(),
                                                        join_err
                                                    )],
                                                }),
                                            }),
                                        }),
                                    ))
                                    .await;
                            }
                            Ok((task_id, Err(peer_err))) => {
                                let _ = peer_srv
                                    .call((
                                        client_node,
                                        Data::ClientQueryData(ClientQueryData {
                                            //[!] ClientQueryData is the only (used) Data-type RPC
                                            token: ser_token,
                                            files: Vec::new(),
                                            metadata: Some(ResponseMetadata {
                                                request_uuid: Uuid::now_v7().into(),
                                                return_code: ReturnCode::PeerServiceError as u32,
                                                error_data: Some(ErrorData {
                                                    error_data: vec![format!(
                                                        "[{:?}] Peer error: {:?}",
                                                        node_tasks.get(&task_id).unwrap(),
                                                        peer_err
                                                    )],
                                                }),
                                            }),
                                        }),
                                    ))
                                    .await;
                            }
                            Ok((task_id, Ok(Response::PeerQueryResponse(res)))) => {
                                let node = node_tasks.get(&task_id).unwrap();
                                let res_metadata = res.metadata().unwrap();
                                match parse_code(res_metadata.return_code) {
                                    ReturnCode::Success => {
                                        //[?] Correct way to deserialize res.files into BTreeSet<OrderedFileSummary> ??
                                        match borrow_decode_from_slice::<Vec<OrderedFileSummary>, _>(
                                            &res.files, config,
                                        ) {
                                            Ok((deserialized_files, _)) => {
                                                let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                                        token: ser_token,
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
                                                        metadata: Some(ResponseMetadata {
                                                            return_code: ReturnCode::Success as u32,
                                                            error_data: match res_metadata.error_data {
                                                                Some(err) => {
                                                                    Some(err)
                                                                },
                                                                None => None
                                                            },
                                                            request_uuid: Uuid::now_v7().into(),
                                                        })
                                                    }))).await;
                                            }
                                            Err(e) => {
                                                let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                                                        token: ser_token,
                                                        files: Vec::new(),
                                                        metadata: Some(ResponseMetadata {
                                                            return_code: ReturnCode::PeerServiceError as u32,
                                                            error_data: Some(ErrorData { error_data: vec![format!("[{:?}] Deserialization error: {:?}", node, e)] }),
                                                            request_uuid: Uuid::now_v7().into(),
                                                        })
                                                    }))).await;
                                            }
                                        }
                                    }
                                    _ => {
                                        let error_str = match res_metadata.error_data {
                                            Some(data) => data.error_data.join("\n"),
                                            None => "Unknown error".to_string(),
                                        };
                                        let _ = peer_srv
                                            .call((
                                                client_node,
                                                Data::ClientQueryData(ClientQueryData {
                                                    //[!] ClientQueryData is the only (used) Data-type RPC
                                                    token: ser_token,
                                                    files: Vec::new(),
                                                    metadata: Some(ResponseMetadata {
                                                        return_code: ReturnCode::PeerServiceError
                                                            as u32,
                                                        request_uuid: Uuid::now_v7().into(),
                                                        error_data: Some(ErrorData {
                                                            error_data: vec![format!(
                                                                "[{:?}] Query Error: {:?}",
                                                                node, error_str
                                                            )],
                                                        }),
                                                    }),
                                                }),
                                            ))
                                            .await;
                                    }
                                }
                            }
                            Ok((task_id, Ok(res))) => {
                                let _ = peer_srv
                                    .call((
                                        client_node,
                                        Data::ClientQueryData(ClientQueryData {
                                            //[!] ClientQueryData is the only (used) Data-type RPC
                                            token: ser_token,
                                            files: Vec::new(),
                                            metadata: Some(ResponseMetadata {
                                                request_uuid: Uuid::now_v7().into(),
                                                return_code: ReturnCode::PeerServiceError as u32,
                                                error_data: Some(ErrorData {
                                                    error_data: vec![format!(
                                                        "[{:?}]: Unexpected response type - {:?}",
                                                        node_tasks.get(&task_id).unwrap(),
                                                        res
                                                    )],
                                                }),
                                            }),
                                        }),
                                    ))
                                    .await;
                            }
                        }
                    }
                });
            }
            Ok((token, packets))
        })
    }
}
