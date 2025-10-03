use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, QueryErr, RetrieveErr};
use crate::services::cache::CacheService;
use crate::services::peer::PeerService;
use crate::services::rpc::{DaemonInfo, parse_peer_id, try_get_workspace};
use crate::services::state::StateService;
use crate::sharedref::ImmutRef;
use crate::shelf::file::FileSummary;
use crate::shelf::{ShelfData, ShelfId, ShelfOwner, ShelfType, merge};
use crate::tag::TagId;
use crate::workspace::Workspace;
use arc_swap::Guard;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use ebi_proto::rpc::{
    ClientQuery, ClientQueryData, Data, ErrorData, File, PeerQuery, Request, RequestMetadata,
    Response, ResponseMetadata, ReturnCode, parse_code,
};
use im::{HashMap, HashSet};
use iroh::NodeId;
use rayon::prelude::*;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    vec,
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
    pub state_srv: StateService,
    pub daemon_info: Arc<DaemonInfo>,
}

pub struct Retriever {
    workspace: Guard<Arc<Workspace>>,
    cache: CacheService,
    shelf_owner: ShelfOwner,
    shelf_data: ImmutRef<ShelfData>,
    order: FileOrder,
}

impl Retriever {
    pub fn new(
        workspace: Guard<Arc<Workspace>>,
        cache: CacheService, //[?] Requires lock ??
        shelf_owner: ShelfOwner,
        shelf_data: ImmutRef<ShelfData>,
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

    pub fn get(&self, tag_id: TagId) -> Result<HashSet<OrderedFileSummary>, RetrieveErr> {
        if let Some(tag_ref) = self.workspace.tags.get(&tag_id) {
            if let Some(set) = self.cache.retrieve(tag_ref) {
                Ok(set)
            } else {
                let root_ref = &*self.shelf_data.root;

                let tags = match root_ref.tags.pin().get(tag_ref) {
                    Some(tag_set) => tag_set
                        .pin()
                        .iter()
                        .map(|f| OrderedFileSummary {
                            file_summary: FileSummary::from(&f, self.shelf_owner.clone()),
                            order: self.order.clone(),
                        })
                        .collect::<im::HashSet<OrderedFileSummary>>(),
                    None => im::HashSet::new(),
                };

                // [TODO] handle dtags
                Ok(tags)
            }
        } else {
            Err(RetrieveErr::TagParseError)
        }
    }

    pub fn get_all(&self) -> Result<HashSet<OrderedFileSummary>, RetrieveErr> {
        //[!] Check cache
        let root_ref = &*self.shelf_data.root;
        let tags = root_ref.tags.pin();

        let tags = tags.iter().flat_map(|(tag_ref, set)| {
            let cached = self.cache.retrieve(tag_ref);
            if let Some(cached) = cached {
                cached
            } else {
                set.pin()
                    .iter()
                    .map(|f| OrderedFileSummary {
                        file_summary: FileSummary::from(&f, self.shelf_owner.clone()),
                        order: self.order.clone(),
                    })
                    .collect::<HashSet<OrderedFileSummary>>()
            }
        });
        // [TODO] handle dtags

        Ok(tags.collect())
    }
}

impl Service<PeerQuery> for QueryService {
    type Response = (Vec<OrderedFileSummary>, Vec<String>);
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: PeerQuery) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let node_id = self.daemon_info.id.clone();
        let cache = self.cache.clone();
        Box::pin(async move {
            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            let serialized_query = req.query;
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let (query, _): (Query, usize) = borrow_decode_from_slice(&serialized_query, config)
                .map_err(|_| ReturnCode::MalformedRequest)?; //[!] Decode Error 

            let file_order = query.order.clone();

            let workspace = workspace_ref.load();
            let local_shelves: HashSet<ShelfId> = workspace
                .shelves
                .iter()
                .filter_map(|(_, s)| {
                    match s.shelf_owner {
                        ShelfOwner::Node(node_owner) => {
                            if node_owner == *node_id {
                                Some(s.id)
                            } else {
                                None
                            }
                        }
                        ShelfOwner::Sync(_) => {
                            todo!()
                        } // [?] Should PeerQuery deal with Sync shelves ??
                    }
                })
                .collect();

            let mut local_futures = JoinSet::new();
            for s_id in local_shelves {
                if let Some(shelf) = workspace.shelves.get(&s_id) {
                    let shelf_owner = shelf.shelf_owner.clone();
                    let cache = cache.clone();
                    let file_order = file_order.clone();
                    let mut query = query.clone();
                    match &shelf.shelf_type {
                        ShelfType::Remote => {
                            continue;
                        }
                        ShelfType::Local(data) => {
                            let workspace = workspace_ref.load();
                            let data = data.clone();
                            local_futures.spawn(async move {
                                let retriever =
                                    Retriever::new(workspace, cache, shelf_owner, data, file_order);
                                query.evaluate(retriever)
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

            let mut files: Vec<OrderedFileSummary> = merge(files);
            files.par_sort(); // [TODO] make this sort optional based on req flag / configuration
            Ok((files, errors))
        })
    }
}

impl Service<ClientQuery> for QueryService {
    type Response = (TokenId, u32);
    type Error = ReturnCode; //[!] Query Request Errors
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut state_srv = self.state_srv.clone();
        let peer_srv = self.peer_srv.clone();
        let node_id = self.daemon_info.id.clone();
        let cache = self.cache.clone();
        Box::pin(async move {
            let query_str = req.query;

            let Ok(workspace_ref) = try_get_workspace(&req.workspace_id, &mut state_srv).await
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

            let workspace = workspace_ref.load();
            let mut nodes = HashSet::<NodeId>::new();
            let mut local_shelves = HashSet::<ShelfId>::new();
            for (shelf_id, shelf) in workspace.shelves.iter() {
                let filter = shelf.filter_tags.load();
                match shelf.shelf_owner {
                    ShelfOwner::Node(node_owner) => {
                        if query.may_hold(&filter) {
                            if node_owner != *node_id {
                                nodes.insert(node_owner);
                            } else {
                                local_shelves.insert(*shelf_id);
                            }
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
            //

            struct TaskResult {
                files: Vec<OrderedFileSummary>,
                ret_code: ReturnCode,
                errors: Vec<String>,
            }

            let mut futures = JoinSet::<TaskResult>::new();
            let mut node_tasks = HashMap::<tokio::task::Id, NodeId>::new();

            let peer_query_task = |node_id: NodeId, peer_req: Request| {
                let mut errors = Vec::<String>::new();
                let mut files = Vec::<OrderedFileSummary>::new();
                let mut peer_srv = peer_srv.clone();

                async move {
                    let ret_code = match peer_srv.call((node_id, peer_req)).await {
                        Err(err) => {
                            errors.push(format!("[{:?}] Peer error: {:?}", node_id, err));
                            ReturnCode::PeerServiceError
                        }
                        Ok(Response::PeerQueryResponse(peer_res)) => {
                            let mut res_metadata = peer_res.metadata.unwrap();
                            match parse_code(res_metadata.return_code) {
                                ReturnCode::Success => {
                                    //[?] result might be rerordered by the requesting daemon based on flag or configuration
                                    match borrow_decode_from_slice::<Vec<OrderedFileSummary>, _>(
                                        &peer_res.files,
                                        config,
                                    ) {
                                        Ok((deserialized_files, _)) => {
                                            files = deserialized_files;
                                            if let Some(e) = res_metadata.error_data.as_mut() {
                                                errors.append(&mut e.error_data);
                                            }
                                            ReturnCode::Success
                                        }
                                        Err(e) => {
                                            errors.push(format!(
                                                "[{:?}] Deserialization error: {:?}",
                                                node_id, e
                                            ));
                                            ReturnCode::PeerServiceError
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
                                    return_code
                                }
                            }
                        }
                        Ok(res) => {
                            errors.push(format!(
                                "[{:?}]: Unexpected response type - {:?}",
                                node_id, res
                            ));
                            ReturnCode::PeerServiceError
                        }
                    };
                    TaskResult {
                        files,
                        ret_code,
                        errors,
                    }
                }
            };

            for node in nodes {
                let peer_req = peer_query.clone();
                let id = futures.spawn(peer_query_task(node, peer_req)).id();
                node_tasks.insert(id, node);
            }

            // Prepare a thread for retriever for each local shelf
            // [TODO] thread / retriever heuristics
            let mut local_futures = JoinSet::new();
            for s_id in local_shelves {
                if let Some(shelf) = workspace.shelves.get(&s_id) {
                    let shelf_owner = shelf.shelf_owner.clone();
                    let mut query = query.clone();
                    let workspace_ref = workspace_ref.clone();
                    match &shelf.shelf_type {
                        ShelfType::Remote => {
                            continue;
                        }
                        ShelfType::Local(data) => {
                            let workspace = workspace_ref.load();
                            let data = data.clone();
                            let retriever = Retriever::new(
                                workspace,
                                cache.clone(),
                                shelf_owner,
                                data,
                                file_order.clone(),
                            );
                            local_futures.spawn(async move { query.evaluate(retriever) });
                        }
                    }
                }
            }

            let node = node_id.clone();
            let local_query_task = async move {
                let mut files = Vec::<HashSet<OrderedFileSummary>>::new();
                let mut errors = Vec::<String>::new();

                while let Some(result) = local_futures.join_next().await {
                    match result {
                        Err(err) => errors.push(format!("[{:?}] Thread error: {:?}", node, err)),
                        Ok(result) => match result {
                            Ok(res) => files.push(res),
                            Err(QueryErr::SyntaxError) => {
                                errors.push(format!(
                                    "[{:?}] Query Parse Error ",
                                    node.as_bytes().to_vec()
                                ));
                            }
                            Err(QueryErr::ParseError) => {
                                errors.push(format!(
                                    "[{:?}] Tag Parse Error ",
                                    node.as_bytes().to_vec()
                                ));
                            }
                            Err(QueryErr::RuntimeError(ret_err)) => {
                                errors.push(format!(
                                    "[{:?}] Runtime Error: {:?}",
                                    node.as_bytes().to_vec(),
                                    ret_err
                                ));
                            }
                        },
                    }
                }

                let mut files = merge(files);
                files.par_sort();

                TaskResult {
                    files,
                    ret_code: ReturnCode::Success, // [!] How should errors from different shelves handled?
                    errors,
                }
            };

            let id = futures.spawn(local_query_task).id();
            node_tasks.insert(id, *node_id.clone());

            if req.atomic {
                let mut peer_srv = peer_srv.clone();
                tokio::spawn(async move {
                    let mut files = Vec::<Vec<OrderedFileSummary>>::new();
                    let mut errors = Vec::<String>::new();

                    while let Some(result) = futures.join_next().await {
                        match result {
                            Err(err) => errors.push(format!(
                                "[{:?}] Thread error: {:?}",
                                node_tasks.get(&err.id()).unwrap(),
                                err
                            )),
                            Ok(mut t_res) => {
                                errors.append(&mut t_res.errors);
                                files.push(t_res.files);
                            }
                        }
                    }

                    let mut files: Vec<OrderedFileSummary> =
                        files.into_par_iter().flat_map(|v| v).collect();

                    files.par_sort(); // if files are already sorted, very cheap to resort

                    peer_srv
                        .call((
                            client_node,
                            Data::ClientQueryData(ClientQueryData {
                                //[!] ClientQueryData is the only (used) Data-type RPC
                                token: ser_token,
                                files: files
                                    .into_iter()
                                    .map(|f| File {
                                        path: f.file_summary.path.to_string_lossy().to_string(),
                                        metadata: Some(f.file_summary.metadata.into()),
                                    })
                                    .collect(),
                                metadata: Some(ResponseMetadata {
                                    request_uuid: Uuid::now_v7().into(),
                                    return_code: ReturnCode::Success as u32, // [?] Should this always be success ??
                                    error_data: Some(ErrorData { error_data: errors }),
                                }),
                            }),
                        ))
                        .await
                });
            } else {
                let mut peer_srv = peer_srv.clone();
                tokio::spawn(async move {
                    while let Some(result) = futures.join_next().await {
                        match result {
                            Err(thread_err) => {
                                let _ = peer_srv
                                    .call((
                                        client_node,
                                        Data::ClientQueryData(ClientQueryData {
                                            //[!] ClientQueryData is the only (used) Data-type RPC
                                            token: ser_token.clone(),
                                            files: Vec::new(),
                                            metadata: Some(ResponseMetadata {
                                                request_uuid: Uuid::now_v7().into(),
                                                return_code: ReturnCode::PeerServiceError as u32,
                                                error_data: Some(ErrorData {
                                                    error_data: vec![format!(
                                                        "[{:?}] Thread error: {:?}",
                                                        node_tasks.get(&thread_err.id()).unwrap(),
                                                        thread_err
                                                    )],
                                                }),
                                            }),
                                        }),
                                    ))
                                    .await;
                            }
                            Ok(t_res) => {
                                let _ = peer_srv
                                    .call((
                                        client_node,
                                        Data::ClientQueryData(ClientQueryData {
                                            //[!] ClientQueryData is the only (used) Data-type RPC
                                            token: ser_token.clone(),
                                            files: t_res
                                                .files
                                                .iter()
                                                .map(|f| File {
                                                    path: f
                                                        .file_summary
                                                        .path
                                                        .to_string_lossy()
                                                        .to_string(),
                                                    metadata: Some(
                                                        f.file_summary.metadata.clone().into(),
                                                    ),
                                                })
                                                .collect(),
                                            metadata: Some(ResponseMetadata {
                                                return_code: t_res.ret_code as u32,
                                                error_data: Some(ErrorData {
                                                    error_data: t_res.errors,
                                                }),
                                                request_uuid: Uuid::now_v7().into(),
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
