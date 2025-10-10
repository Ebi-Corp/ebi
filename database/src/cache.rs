use ebi_types::file::{FileOrder, OrderedFileSummary};
use ebi_types::shelf::ShelfId;
use ebi_types::tag::{TagId, TagRef};
use ebi_types::workspace::WorkspaceId;
use im::HashSet;

#[derive(Clone, Debug)]
pub struct CacheService {
    //[TODO] Design Cache Structure
}

impl CacheService {
    pub fn retrieve(&self, _tag_ref: &TagRef) -> Option<HashSet<OrderedFileSummary>> {
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
