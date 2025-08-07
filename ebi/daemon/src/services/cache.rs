use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::services::peer::PeerService;
use crate::services::rpc::{DaemonInfo};
use crate::services::workspace::{WorkspaceService};
use crate::shelf::shelf::{ShelfId};
use crate::tag::{TagId, TagRef};
use crate::workspace::{WorkspaceId};
use std::collections::{BTreeSet};
use std::sync::Arc;

#[derive(Clone)]
pub struct CacheService {
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
    daemon_info: Arc<DaemonInfo>,
}

impl CacheService {
    pub fn retrieve(&self, tag_ref: &TagRef) -> Option<BTreeSet<OrderedFileSummary>> {
        None //[TODO] Implement caching 
    }
}

pub enum RetrieveFiles {
    GetAll(WorkspaceId, FileOrder),
    GetTagged(WorkspaceId, FileOrder, Vec<ShelfId>, TagId),
}

enum Caching {
    IsCacheValid(Option<TagId>, HashCache),
}

struct HashCache {
    hash: u64,
}

#[derive(Debug)]
pub enum CacheError {
    WorkspaceNotFound,
}

pub type CacheServiceRef = Arc<CacheService>;
