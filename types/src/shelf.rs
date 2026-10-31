use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::NodeId;
use crate::Ref;
use crate::Uuid;
use crate::stateful::Revision;
use crate::tag::Tag;
use crate::{SharedRef, Stamped, StatefulRef};
use arc_swap::ArcSwap;
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

pub type ShelfId = Uuid;
pub type TagRef = SharedRef<Tag>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ShelfType {
    Local,
    Remote,
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum ShelfOwner {
    Node(NodeId),
    Sync(SyncId),
}

//[#] Sync

pub type SyncId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncConfig {
    //[!] Placeholder for sync configuration
    pub interval: Option<Duration>, // Auto-Sync Interval
    pub auto_sync: bool,            // Auto-Sync on changes
}

//[#] Sync

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ShelfConfig {
    //[TODO] Define a configuration for the shelf
    pub sync_config: Option<SyncConfig>,
}

#[derive(Debug)]
pub struct Shelf<TagFilter> {
    pub shelf_type: ShelfType,
    pub shelf_owner: ShelfOwner,
    pub config: ShelfConfig,
    pub filter_tags: ArcSwap<TagFilter>,
    pub info: StatefulRef<ShelfInfo, ()>,
}

impl<TagFilter> Clone for Shelf<TagFilter> {
    fn clone(&self) -> Self {
        Shelf {
            shelf_type: self.shelf_type.clone(),
            shelf_owner: self.shelf_owner.clone(),
            config: self.config.clone(),
            filter_tags: ArcSwap::new(self.filter_tags.load_full()),
            info: self.info.clone_inner(),
        }
    }
}

impl<TagFilter: Default> Shelf<TagFilter> {
    pub fn new(
        path: PathBuf,
        name: String,
        shelf_type: ShelfType,
        shelf_owner: ShelfOwner,
        config: Option<ShelfConfig>,
        description: String,
    ) -> Self {
        Shelf {
            shelf_type,
            shelf_owner,
            config: config.unwrap_or_default(),
            filter_tags: ArcSwap::new(Arc::new(TagFilter::default())), // [TODO] Filter parameters (size, ...) should be configurable
            info: StatefulRef::new_ref((), ShelfInfo::new(Some(name), Some(description), path)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShelfInfo {
    pub name: Stamped<String>,
    pub description: Stamped<String>,
    pub root: PathBuf, //[?] Immutable ??
}

// Does not compare timestamps 
impl PartialEq for ShelfInfo {
    fn eq(&self, other: &Self) -> bool {
        self.name.get() == other.name.get()
            && self.description.get() == other.description.get()
            && self.root == other.root
    }
}

impl Eq for ShelfInfo {}

impl ShelfInfo {
    pub fn new(name: Option<String>, description: Option<String>, root_path: PathBuf) -> Self {
        let default_name = root_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("Unnamed"))
            .to_string_lossy()
            .to_string();
        let at = Utc::now();
        ShelfInfo {
            name: Stamped::new(name.unwrap_or(default_name), at),
            description: Stamped::new(description.unwrap_or_default(), at),
            root: root_path,
        }
    }

    pub fn edited(&self, name: String, description: String, at: Revision) -> Self {
        ShelfInfo {
            name: self.name.set(name, at),
            description: self.description.set(description, at),
            root: self.root.clone(),
        }
    }

    // Idempotency allows merging without tracking applied edits
    pub fn merge(&self, other: &Self) -> Self {
        ShelfInfo {
            name: self.name.merge(&other.name),
            description: self.description.merge(&other.description),
            root: self.root.clone() // `root` is immutable, so it is not merged
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    fn at(secs: i64) -> Revision {
        DateTime::<Utc>::UNIX_EPOCH + Duration::seconds(secs)
    }

    fn info(name: &str, description: &str, base: i64) -> ShelfInfo {
        ShelfInfo {
            name: Stamped::new(name.to_string(), at(base)),
            description: Stamped::new(description.to_string(), at(base)),
            root: PathBuf::from("/shelf"),
        }
    }

    // Future behaviour being equal requires identical timestamps, but `PartialEq` only compares values
    fn full(i: &ShelfInfo) -> (String, String, Revision, Revision) {
        (
            i.name.get().clone(),
            i.description.get().clone(),
            i.name.rev(),
            i.description.rev(),
        )
    }

    #[test]
    fn merge_concurrent_edits() {
        let base = info("n", "d", 10);
        let renamed = base.edited("renamed".to_string(), "d".to_string(), at(20));
        let redescribed = base.edited("n".to_string(), "redescribed".to_string(), at(21));

        let merged = renamed.merge(&redescribed);
        assert_eq!(merged.name.get(), "renamed");
        assert_eq!(merged.description.get(), "redescribed");
        assert_eq!(merged.name.rev(), at(20));
        assert_eq!(merged.description.rev(), at(21));
    }

    #[test]
    fn merge_idempotent() {
        let base = info("n", "d", 10);
        let a = base.edited("a".to_string(), "d".to_string(), at(20));
        assert_eq!(full(&a.merge(&a)), full(&a), "Merge is not idempotent");
    }

    #[test]
    fn merge_commutative() {
        let base = info("n", "d", 10);
        let a = base.edited("a".to_string(), "d".to_string(), at(20));
        let b = base.edited("n".to_string(), "b".to_string(), at(21));
        assert_eq!(full(&a.merge(&b)), full(&b.merge(&a)), "Merge is not commutative");
    }

    #[test]
    fn merge_associative() {
        let base = info("n", "d", 10);
        let a = base.edited("a".to_string(), "d".to_string(), at(20));
        let b = base.edited("n".to_string(), "b".to_string(), at(21));
        let c = base.edited("c".to_string(), "c".to_string(), at(25));
        assert_eq!(
            full(&a.merge(&b).merge(&c)),
            full(&a.merge(&b.merge(&c))),
            "Merge is not associative"
        );
    }

    #[test]
    fn merge_preserves_root() {
        let a = info("a", "d", 20);
        let b = info("b", "d", 21);
        assert_eq!(a.merge(&b).root, PathBuf::from("/shelf"));
    }
}
