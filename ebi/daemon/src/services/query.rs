use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, QueryErr, RetrieveErr};
use crate::services::cache::CacheService;
use crate::services::peer::{PeerError, PeerService};
use crate::services::rpc::{DaemonInfo, parse_peer_id, try_get_workspace};
use crate::services::workspace::WorkspaceService;
use crate::shelf::file::FileSummary;
use crate::shelf::{ShelfDataRef, ShelfId, ShelfOwner, ShelfType, merge};
use crate::tag::TagId;
use crate::workspace::WorkspaceRef;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use chrono::{DateTime, Utc};
use ebi_proto::rpc::{
    ClientQuery, ClientQueryData, Data, Empty, ErrorData, File, FileMetadata, PeerQuery, Request,
    RequestMetadata, ResMetadata, Response, ResponseMetadata, ReturnCode, UnixMetadata,
    WindowsMetadata, parse_code,
};
use iroh::NodeId;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::vec;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::JoinSet;
use tower::Service;
use uuid::Uuid;

type TaskID = u64;
pub type TokenId = Uuid;

#[derive(Clone)]
pub struct QueryService {
    pub peer_srv: PeerService,
    pub cache: CacheService,
    pub workspace_srv: WorkspaceService,
    pub daemon_info: Arc<DaemonInfo>,
}

#[derive(Clone)]
pub struct Retriever {
    workspace: WorkspaceRef,
    cache: CacheService,
    shelf_owner: ShelfOwner,
    shelf_data: ShelfDataRef,
    order: FileOrder,
}

impl Retriever {
    pub fn new(
        workspace: WorkspaceRef,
        cache: CacheService, //[?] Requires lock ??
        shelf_owner: ShelfOwner,
        shelf_data: ShelfDataRef,
        order: FileOrder,
    ) -> Retriever {
        Retriever {
            workspace,
            cache,
            shelf_owner,
            shelf_data,
            order,
        }
    }

    pub async fn get(&self, tag_id: TagId) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        if let Some(tag_ref) = self.workspace.read().await.tags.get(&tag_id) {
            if let Some(set) = self.cache.retrieve(tag_ref) {
                Ok(set) //[?] What is CacheError supposed to mean ?? 
            } else {
                let shelf_r = self.shelf_data.read().await;
                let shelf_owner = self.shelf_owner.clone();
                let mut tags = if let Some(tag_set) = shelf_r.root.tags.get(tag_ref) {
                    tag_set
                        .iter()
                        .map(|f| OrderedFileSummary {
                            file_summary: FileSummary::from(f.clone(), shelf_owner.clone()),
                            order: self.order.clone(),
                        })
                        .collect::<BTreeSet<OrderedFileSummary>>()
                } else {
                    BTreeSet::new()
                };

                let mut dtags = if let Some(dtag_set) = shelf_r.root.dtag_files.get(tag_ref) {
                    dtag_set
                        .iter()
                        .map(|f| OrderedFileSummary {
                            file_summary: FileSummary::from(f.clone(), shelf_owner.clone()),
                            order: self.order.clone(),
                        })
                        .collect::<BTreeSet<OrderedFileSummary>>()
                } else {
                    BTreeSet::new()
                };

                drop(shelf_r);
                if tags.len() >= dtags.len() {
                    tags.extend(dtags);
                    Ok(tags)
                } else {
                    dtags.extend(tags);
                    Ok(dtags)
                }
            }
        } else {
            Err(RetrieveErr::TagParseError)
        }
    }

    pub async fn get_all(&self) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        //[!] Check cache
        let shelf_r = self.shelf_data.read().await;
        let shelf_owner = self.shelf_owner.clone();
        let tags = shelf_r.root.tags.iter().flat_map(|(tag_ref, set)| {
            let cached = self.cache.retrieve(tag_ref);
            if let Some(cached) = cached {
                cached
            } else {
                set.iter()
                    .map(|f| OrderedFileSummary {
                        file_summary: FileSummary::from(f.clone(), shelf_owner.clone()),
                        order: self.order.clone(),
                    })
                    .collect::<BTreeSet<OrderedFileSummary>>()
            }
        });
        let dtags = shelf_r.root.dtag_files.iter().flat_map(|(tag_ref, set)| {
            let cached = self.cache.retrieve(tag_ref);
            if let Some(cached) = cached {
                cached
            } else {
                set.iter()
                    .map(|f| OrderedFileSummary {
                        file_summary: FileSummary::from(f.clone(), shelf_owner.clone()),
                        order: self.order.clone(),
                    })
                    .collect::<BTreeSet<OrderedFileSummary>>()
            }
        });
        Ok(tags.chain(dtags).collect())
    }
}

impl Service<PeerQuery> for QueryService {
    type Response = (BTreeSet<OrderedFileSummary>, Vec<String>);
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: PeerQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let node_id = self.daemon_info.id;
        let cache = self.cache.clone();
        Box::pin(async move {
            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            let serialized_query = req.query;
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let (query, _): (Query, usize) = borrow_decode_from_slice(&serialized_query, config)
                .map_err(|_| ReturnCode::MalformedRequest)?; //[!] Decode Error 

            let file_order = query.order.clone();

            let shelves = &workspace.read().await.shelves;
            let mut local_shelves = HashSet::<ShelfId>::new();
            for (shelf_id, shelf) in shelves {
                let shelf_r = shelf.read().await;
                let shelf_owner = shelf_r.shelf_owner.clone();
                drop(shelf_r);
                match shelf_owner {
                    ShelfOwner::Node(node_owner) => {
                        if node_owner == node_id {
                            local_shelves.insert(*shelf_id);
                        }
                    }
                    ShelfOwner::Sync(_) => {} // [?] Should PeerQuery deal with Sync shelves ??
                }
            }

            let mut local_futures = JoinSet::new();
            for s_id in local_shelves {
                let workspace = workspace.clone();
                let workspace_r = workspace.read().await;
                let shelf = workspace_r.shelves.get(&s_id);
                if let Some(shelf) = shelf {
                    let shelf_r = shelf.read().await;
                    let shelf_type = shelf_r.shelf_type.clone();
                    let workspace = workspace.clone();
                    let cache = cache.clone();
                    let shelf_owner = shelf_r.shelf_owner.clone();
                    let file_order = file_order.clone();
                    let mut query = query.clone();
                    match shelf_type {
                        ShelfType::Remote => {
                            continue;
                        }
                        ShelfType::Local(data) => {
                            local_futures.spawn(async move {
                                let retriever = Retriever::new(
                                    workspace,
                                    cache,
                                    shelf_owner,
                                    data.clone(),
                                    file_order,
                                );
                                query.evaluate(retriever).await
                            });
                        }
                    }
                }
            }

            let mut errors = Vec::new();
            let mut files = Vec::new();

            while let Some(result) = local_futures.join_next().await {
                match result {
                    Err(err) => errors.push(format!("[{:?}] Thread error: {:?}", node_id, err)),
                    Ok(result) => match result {
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
                    },
                }
            }

            let files: BTreeSet<OrderedFileSummary> = merge(files);
            Ok((files, errors))
        })
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
        let node_id = self.daemon_info.id;
        let cache = self.cache.clone();
        Box::pin(async move {
            let query_str = req.query;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            let Some(metadata) = req.metadata else {
                return Err(ReturnCode::MalformedRequest);
            };

            let client_node = parse_peer_id(&metadata.source_id).unwrap();

            let file_order = FileOrder::try_from(req.file_ord.unwrap().order_by)?;
            let mut query = Query::new(
                &query_str,
                file_order.clone(),
                req.file_ord.unwrap().ascending,
            )
            .map_err(|_| ReturnCode::InternalStateError)?;

            let shelves = &workspace.read().await.shelves;
            let mut nodes = HashSet::<NodeId>::new();
            let mut local_shelves = HashSet::<ShelfId>::new();
            for (shelf_id, shelf) in shelves {
                let shelf_r = shelf.read().await;
                let filter = shelf_r.filter_tags.clone();
                let shelf_owner = shelf_r.shelf_owner.clone();
                drop(shelf_r);
                match shelf_owner {
                    ShelfOwner::Node(node_owner) => {
                        if node_owner != node_id {
                            if query.may_hold(&filter) {
                                nodes.insert(node_owner);
                            }
                        } else {
                            local_shelves.insert(*shelf_id);
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

            let mut futures = JoinSet::<Result<Response, PeerError>>::new(); //[?] Move into Local/Remote Check Logic ??
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

            //Local Query
            let mut local_futures = JoinSet::new();
            for s_id in local_shelves {
                let workspace = workspace.clone();
                let workspace_r = workspace.read().await;
                let shelf = workspace_r.shelves.get(&s_id);
                if let Some(shelf) = shelf {
                    let shelf_r = shelf.read().await;
                    let shelf_type = shelf_r.shelf_type.clone();
                    let workspace = workspace.clone();
                    let cache = cache.clone();
                    let shelf_owner = shelf_r.shelf_owner.clone();
                    let file_order = file_order.clone();
                    let mut query = query.clone();
                    match shelf_type {
                        ShelfType::Remote => {
                            continue;
                        }
                        ShelfType::Local(data) => {
                            local_futures.spawn(async move {
                                let retriever = Retriever::new(
                                    workspace,
                                    cache,
                                    shelf_owner,
                                    data.clone(),
                                    file_order,
                                );
                                query.evaluate(retriever).await
                            });
                        }
                    }
                }
            }

            if req.atomic {
                tokio::spawn(async move {
                    let mut files = Vec::<BTreeSet<OrderedFileSummary>>::new();
                    let mut errors = Vec::<String>::new();

                    while let Some(result) = local_futures.join_next().await {
                        match result {
                            Err(err) => {
                                errors.push(format!("[{:?}] Thread error: {:?}", node_id, err))
                            }
                            Ok(result) => match result {
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
                            },
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
                    let files: BTreeSet<OrderedFileSummary> = merge(files);

                    let _ = peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only (used) Data-type RPC 
                            token: ser_token,
                            files: files.into_iter().map(|f| File {
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
                    while let Some(result) = local_futures.join_next().await {
                        match result {
                            Ok(result) => {
                                match result {
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
                                                format!(
                                                    "[{:?}] Tag Parse Error",
                                                    node_id.as_bytes().to_vec()
                                                )
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
                            }
                            Err(err) => {
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
                                                    error_data: vec![format!(
                                                        "[{:?}] Join Error: {:?}",
                                                        node_id.as_bytes().to_vec(),
                                                        err.to_string()
                                                    )],
                                                }),
                                            }),
                                        }),
                                    ))
                                    .await;
                            }
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
                                                            error_data: res_metadata.error_data,
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
