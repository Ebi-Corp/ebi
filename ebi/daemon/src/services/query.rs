use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, RetrieveErr, RetrieveService};
use crate::services::cache::CacheService;
use crate::services::peer::PeerService;
use crate::services::rpc::{try_get_workspace, uuid};
use crate::services::workspace::WorkspaceService;
use crate::shelf::shelf::{ShelfOwner, ShelfType};
use crate::tag::TagId;
use crate::workspace::WorkspaceId;
use bincode::{serde::decode_from_slice, serde::encode_to_vec};
use ebi_proto::rpc::{
    ClientQuery, PeerQuery, ReqMetadata, Request, ResMetadata, ReturnCode, parse_code,
};
use iroh::NodeId;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::JoinHandle;
use tower::Service;

type TaskID = u64;

#[derive(Clone)]
struct QueryService {
    retrieve_serv: Retrieve,
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
    tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,
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

impl Service<ClientQuery> for QueryService {
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        let _ = Box::pin(async move {
            let query_str = req.query;
            //let ord = to_file_order(&req.file_ord.unwrap());
            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };
            let workspace_id = uuid(req.workspace_id.clone()).unwrap();
            let shelves = &workspace.read().await.shelves;
            let mut nodes = HashSet::<NodeId>::new();
            for (_, shelf) in shelves {
                let shelf_r = shelf.read().await;
                match shelf_r.shelf_type {
                    ShelfType::Local(_) => {}
                    ShelfType::Remote => {
                        match shelf.read().await.shelf_owner {
                            ShelfOwner::Node(node_id) => {
                                nodes.insert(node_id);
                            }
                            ShelfOwner::Sync(_) => {} // [!] Implement Sync logic
                        }
                    }
                }
            }

            let file_order = FileOrder::try_from(req.file_ord.unwrap().order_by)?;
            let mut query = Query::new(&query_str, file_order, req.file_ord.unwrap().ascending)
                .map_err(|_| ReturnCode::InternalStateError)?;
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let mut serialized_query =
                encode_to_vec(&query, config).map_err(|_| ReturnCode::ParseError)?;

            let peer_req = Request::from(PeerQuery {
                query: serialized_query,
                workspace_id: req.workspace_id,
                metadata: req.metadata,
            });

            // Build PeerQuery request

            if !req.partial {
                for node_id in nodes {
                    match peer_srv.call((node_id, peer_req.clone())).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                    };
                }
            }

            // Ok until now
            //query.evaluate(uuid(req.workspace_id).unwrap(), self.retrieve_serv).await.map_err(|_| ReturnCode::InternalStateError)?;

            let _join_handle: JoinHandle<()> = tokio::spawn(async move {
                //query.evaluate(self.retrieve_serv);
                //peer_service.call();
            });
            Ok(())
        });
        todo!();
    }
}
