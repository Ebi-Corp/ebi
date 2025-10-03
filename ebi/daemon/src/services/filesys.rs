use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::prelude::*;
use crate::shelf::ShelfDataRef;
use crate::shelf::node::Node;
use ebi_proto::rpc::ReturnCode;
use file_id::{FileId, get_file_id};
use papaya::HashSet;
use tower::Service;

#[derive(Clone)]
pub struct FileSysService {
    pub nodes: Arc<HashSet<ImmutRef<Node, FileId>>>,
}
struct NodeKey {
    id: FileId,
    path: PathBuf,
}

impl PartialEq for NodeKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

struct GetInitNode(ShelfDataRef, PathBuf);

impl FileSysService {
    pub async fn get_or_init_node(
        &mut self,
        shelf: ShelfDataRef,
        path: PathBuf,
    ) -> Result<FileId, ReturnCode> {
        self.call(GetInitNode(shelf, path)).await
    }
}

impl Service<GetInitNode> for FileSysService {
    type Response = FileId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetInitNode) -> Self::Future {
        let nodes = self.nodes.clone();
        Box::pin(async move {
            let shelf = req.0;
            let r_path = req.1;
            if r_path.starts_with(&shelf.root_path) {
                return Err(ReturnCode::PathNotFound);
            }
            let path = if !r_path.is_dir() {
                let Some(path) = r_path.parent() else {
                    return Err(ReturnCode::PathNotFound);
                };
                path.to_owned()
            } else {
                r_path
            };
            let mut new_subnode: Option<ImmutRef<Node, FileId>> = None;
            let mut trav_path = path.clone();
            let Ok(nfile_id) = get_file_id(&trav_path) else {
                return Err(ReturnCode::InternalStateError);
            };
            loop {
                let Ok(file_id) = get_file_id(&trav_path) else {
                    return Err(ReturnCode::InternalStateError);
                };
                let node = {
                    if let Some(node) = nodes.pin().get(&file_id) {
                        node.clone()
                    } else {
                        let Ok(node) = Node::new(path.clone()) else {
                            return Err(ReturnCode::InternalStateError);
                        };
                        let node_ref = ImmutRef::<Node, FileId>::new_ref_id(file_id, node);
                        nodes.pin().insert(node_ref.clone());
                        node_ref
                    }
                };
                let node_wref = node.downgrade();

                if let Some(subnode) = new_subnode {
                    subnode.subdirs.pin().insert(node_wref.clone());
                }

                new_subnode = Some(node.clone());

                // if the node already existed in the shelf, we are done
                if !shelf.nodes.pin().insert(node_wref.clone()) {
                    break Ok(nfile_id);
                }
                trav_path = trav_path.parent().unwrap().to_path_buf();
            }
        })
    }
}
