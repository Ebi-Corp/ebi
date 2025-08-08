use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::shelf::ShelfId;
use crate::tag::{TagId, TagRef};
use crate::workspace::WorkspaceId;
use std::collections::BTreeSet;

#[derive(Clone)]
pub struct CacheService {
    //[TODO] Design Cache Structure
}

impl CacheService {
    pub fn retrieve(&self, _tag_ref: &TagRef) -> Option<BTreeSet<OrderedFileSummary>> {
        None //[TODO] Implement cache retrieval 
    }

    pub fn store(&self, _tag_ref: &TagRef) -> Result<bool, CacheError> {
        Ok(false) //[TODO] Implement caching 
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
