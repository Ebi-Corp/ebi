use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::services::peer::PeerService;
use crate::services::workspace::{WorkspaceService, GetShelf, GetTag};
use crate::shelf::shelf::{ShelfId, ShelfRef, ShelfType};
use crate::shelf::file::{FileRef, FileSummary};
use crate::tag::TagId;
use crate::workspace::{Workspace, WorkspaceId};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::RwLock,
    task::{Context, Poll},
};
use tower::Service;

#[derive(Clone)]
pub struct CacheService {
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
}

pub enum RetrieveFiles {
    GetAll(WorkspaceId, FileOrder),
    GetTagged(WorkspaceId, FileOrder, Vec<ShelfId>, TagId),
}

enum Caching {
    IsCacheValid(Option<TagId>, HashCache),
}
enum RetrieveData {
    GetDir(PathBuf),
    GetFile(PathBuf),
}
enum RetrieveInfo {
    GetFileInfo(PathBuf),
    GetDirInfo(PathBuf),
}

struct HashCache {
    hash: u64,
}

enum CommandRes {
    OrderedFiles(BTreeSet<OrderedFileSummary>),
}

#[derive(Debug)]
pub enum CacheError {
    WorkspaceNotFound,
}

impl Service<RetrieveFiles> for CacheService {
    type Response = BTreeSet<OrderedFileSummary>;
    type Error = CacheError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RetrieveFiles) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        Box::pin(async move {
            match req {
                RetrieveFiles::GetAll(work_id, ord) => {
                    todo!();
                }

                RetrieveFiles::GetTagged(workspace_id, order, shelves, tag_id) => {
                    let mut shelf_refs = Vec::<ShelfRef>::new();
                    let tag_ref = workspace_srv.call(GetTag {
                        workspace_id, tag_id
                    }).await.unwrap();
                    for shelf_id in shelves {
                        shelf_refs.push(
                            workspace_srv
                                .call(GetShelf {
                                    workspace_id,
                                    shelf_id,
                                })
                                .await
                                .unwrap(),
                        )
                    }
                    let mut tagged = BTreeSet::<OrderedFileSummary>::new();
                    for shelf in shelf_refs {
                        let shelf_r = shelf.read().await;
                        let shelf_owner = shelf_r.shelf_owner.clone();
                        let ShelfType::Local(data_ref) = shelf_r.shelf_type.clone() else {
                            return Err(CacheError::WorkspaceNotFound) 
                        };
                        let mut shelf_tagged = data_ref.read().await.retrieve(tag_ref.clone()).await;
                        drop(data_ref);
                        drop(shelf_r);
                        tagged.append(&mut shelf_tagged.into_iter().map(|x| OrderedFileSummary { file_summary: FileSummary::from(x, shelf_owner.clone()), order: order.clone()  }).collect());

                    }
                    Ok(tagged)
                }
            }
        })
    }
}
