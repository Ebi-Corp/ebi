use ebi_database::state::GetWorkspace;
use ebi_filesystem::shelf::{ShelfDataRef, TagFilter};
use ebi_database::{state::StateService, cache::CacheService};
use ebi_types::file::{FileOrder, OrderedFileSummary};
use ebi_types::{shelf::{ShelfId, ShelfOwner, ShelfType}, tag::TagId, workspace::Workspace};
use ebi_types::{Uuid, FileId, NodeId, ImmutRef, parse_peer_id, StatefulRef, uuid};
use ebi_filesystem::{service::FileSystem, file::gen_summary, shelf::ShelfData, shelf::merge};
use ebi_network::service::Network;
use crate::{Query, QueryErr};

use arc_swap::Guard;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use ebi_proto::rpc::{
    ClientQuery, ClientQueryData, Data, ErrorData, File, PeerQuery, Request, RequestMetadata,
    Response, ResponseMetadata, ReturnCode, parse_code,
};
use im::{HashMap, HashSet};
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

type TaskID = u64;
pub type TokenId = Uuid;

pub async fn try_get_workspace(
    rawid: &[u8],
    srv: &mut StateService,
) -> Result<Arc<StatefulRef<Workspace<ShelfDataRef, TagFilter>>>, ReturnCode> {
    let id = uuid(rawid).map_err(|_| ReturnCode::ParseError)?;
    srv.call(GetWorkspace { id }).await
}

#[derive(Clone)]
pub struct QueryService {
    pub network: Network,
    pub cache: CacheService,
    pub state_srv: StateService,
    pub filesys: FileSystem,
    pub daemon_id: Arc<NodeId>,
}

pub struct Retriever {
    workspace: Guard<Arc<Workspace<ShelfDataRef, TagFilter>>>,
    cache: CacheService,
    filesys: FileSystem,
    shelf_owner: ShelfOwner,
    shelf_data: ImmutRef<ShelfData>,
    subpath: Option<FileId>,
    order: FileOrder,
}

impl Retriever {
    pub fn new(
        workspace: Guard<Arc<Workspace<ShelfDataRef, TagFilter>>>,
        cache: CacheService,
        filesys: FileSystem,
        shelf_owner: ShelfOwner,
        shelf_data: ImmutRef<ShelfData>,
        subpath: Option<FileId>,
        order: FileOrder,
    ) -> Retriever {
        Retriever {
            workspace,
            cache,
            filesys,
            shelf_owner,
            shelf_data,
            subpath,
            order,
        }
    }

    pub async fn get(&mut self, tag_id: TagId) -> Result<HashSet<OrderedFileSummary>, ReturnCode> {
        if let Some(tag_ref) = self.workspace.tags.get(&tag_id) {
            if let Some(set) = self.cache.retrieve(tag_ref) {
                Ok(set)
            } else {
                let dir_id = if self.subpath.is_some() {
                    self.subpath.unwrap()
                } else {
                    self.shelf_data.root.id
                };

                let Some(sdir_ref) = self
                    .shelf_data
                    .dirs
                    .pin()
                    .get(&dir_id)
                    .and_then(|n| n.upgrade())
                else {
                    return Err(ReturnCode::PathNotFound);
                };

                let dtags = sdir_ref.dtags.pin_owned();
                let root_dtag = dtags.get(tag_ref).clone();

                if root_dtag.is_some() {
                    Ok(self
                        .filesys
                        .retrieve_dir_recursive(sdir_ref.path.clone(), self.order.clone())
                        .await?)
                } else {
                    let mut files = match sdir_ref.tags.pin_owned().get(tag_ref) {
                        Some(tag_set) => tag_set
                            .pin()
                            .iter()
                            .map(|f| OrderedFileSummary {
                                file_summary: gen_summary(&f, Some(self.shelf_owner.clone())),
                                order: self.order.clone(),
                            })
                            .collect::<im::HashSet<OrderedFileSummary>>(),
                        None => im::HashSet::new(),
                    };
                    if let Some(sub_dtagged) = sdir_ref.dtag_dirs.pin_owned().get(tag_ref) {
                        for subdir in sub_dtagged {
                            if let Some(subdir) = subdir.upgrade() {
                                files = files.union(
                                    self.filesys
                                        .retrieve_dir_recursive(
                                            subdir.path.clone(),
                                            self.order.clone(),
                                        )
                                        .await?,
                                );
                            }
                        }
                    }
                    Ok(files)
                }
            }
        } else {
            Err(ReturnCode::TagNotFound)
        }
    }

    pub async fn get_all(&mut self) -> Result<HashSet<OrderedFileSummary>, ReturnCode> {
        let dir_id = if self.subpath.is_some() {
            self.subpath.unwrap()
        } else {
            self.shelf_data.root.id
        };

        // [TODO] handle caching
        let Some(sdir_ref) = self
            .shelf_data
            .dirs
            .pin()
            .get(&dir_id)
            .and_then(|n| n.upgrade())
        else {
            return Err(ReturnCode::PathNotFound);
        };
        let result = self
            .filesys
            .retrieve_dir_recursive(sdir_ref.path.clone(), self.order.clone())
            .await?;
        Ok(result)
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
        let mut filesys = self.filesys.clone();
        let node_id = self.daemon_id.clone();
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
            let mut q_dir_id: Option<FileId> = None;

            let workspace = workspace_ref.load();
            let mut local_shelves = HashSet::<ShelfId>::new();
            for (s_id, shelf) in workspace.shelves.iter() {
                match shelf.shelf_owner {
                    ShelfOwner::Node(node_owner) if node_owner == *node_id => {
                        if let Some(ref subpath) = query.path {
                            let shelf_root = &shelf.info.load().root;
                            if subpath
                                .to_string_lossy()
                                .starts_with(shelf_root.to_str().unwrap())
                            {
                                match &shelf.shelf_type {
                                    ShelfType::Local(shelf_data) => {
                                        q_dir_id = Some(
                                            filesys
                                                .get_or_init_dir(shelf_data.clone(), subpath.into())
                                                .await?,
                                        );
                                        local_shelves.insert(*s_id);
                                    }
                                    ShelfType::Remote => return Err(ReturnCode::PathNotFound),
                                }
                                break;
                            }
                        } else {
                            local_shelves.insert(*s_id);
                        }
                    }
                    ShelfOwner::Node(_) => continue,
                    ShelfOwner::Sync(_) => {
                        todo!()
                    } // [?] Should PeerQuery deal with Sync shelves ??
                }
            }
            if local_shelves.is_empty() {
                return Err(ReturnCode::PathNotFound);
            }

            let mut local_futures = JoinSet::new();
            for s_id in local_shelves {
                if let Some(shelf) = workspace.shelves.get(&s_id) {
                    let shelf_owner = shelf.shelf_owner.clone();
                    let cache = cache.clone();
                    let file_order = file_order.clone();
                    let filesys = filesys.clone();
                    let mut query = query.clone();
                    match &shelf.shelf_type {
                        ShelfType::Remote => {
                            continue;
                        }
                        ShelfType::Local(data) => {
                            let workspace = workspace_ref.load();
                            let data = data.clone();
                            local_futures.spawn(async move {
                                let retriever = Retriever::new(
                                    workspace,
                                    cache,
                                    filesys,
                                    shelf_owner,
                                    data,
                                    q_dir_id,
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
        let network = self.network.clone();
        let node_id = self.daemon_id.clone();
        let cache = self.cache.clone();
        let mut filesys = self.filesys.clone();
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
            let mut q_dir_id: Option<FileId> = None;
            let mut to_query: Option<NodeId> = None;
            let mut nodes = HashSet::<NodeId>::new();
            let mut local_shelves = HashSet::<ShelfId>::new();

            if let Some(ref dir_path) = req.path {
                for (_, shelf) in workspace_ref.load().shelves.iter() {
                    let shelf_root = &shelf.info.load().root;
                    if dir_path
                        .to_string()
                        .starts_with(shelf_root.to_str().unwrap())
                    {
                        match &shelf.shelf_type {
                            ShelfType::Local(shelf_data) => {
                                q_dir_id = Some(
                                    filesys
                                        .get_or_init_dir(shelf_data.clone(), dir_path.into())
                                        .await?,
                                );
                            }
                            ShelfType::Remote => match shelf.shelf_owner {
                                ShelfOwner::Node(node_id) => to_query = Some(node_id),
                                ShelfOwner::Sync(_sync_id) => todo!(),
                            },
                        }
                        break;
                    }
                }
                if q_dir_id.is_none() && to_query.is_none() {
                    return Err(ReturnCode::PathNotFound); // [TODO] may need a better error. like path is not in workspace
                }
            }

            let mut query = Query::new(
                req.path.map(|p| p.into()),
                &query_str,
                file_order.clone(),
                req.file_ord.unwrap().ascending,
            )
            .map_err(|_| ReturnCode::InternalStateError)?;

            let workspace = workspace_ref.load();
            if to_query.is_none() {
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
                let mut network = network.clone();

                async move {
                    let ret_code = match network.send_request(node_id, peer_req).await {
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
                                filesys.clone(),
                                shelf_owner,
                                data,
                                q_dir_id,
                                file_order.clone(),
                            );
                            local_futures.spawn(async move { query.evaluate(retriever).await });
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
                let mut network = network.clone();
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

                    network
                        .send_data(
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
                        )
                        .await
                });
            } else {
                let mut network = network.clone();
                tokio::spawn(async move {
                    while let Some(result) = futures.join_next().await {
                        match result {
                            Err(thread_err) => {
                                let _ = network
                                    .send_data(
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
                                    )
                                    .await;
                            }
                            Ok(t_res) => {
                                let _ = network
                                    .send_data(
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
                                    )
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
