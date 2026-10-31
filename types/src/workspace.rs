use crate::Uuid;
use crate::shelf::{Shelf, ShelfId};
use crate::stateful::Revision;
use crate::tag::{Tag, TagId};
use crate::{ImmutRef, SharedRef, Stamped, StatefulMap, StatefulRef};
use chrono::Utc;

pub type WorkspaceId = Uuid;

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Workspace<TagFilter> {
    // Workspace Info
    pub info: StatefulRef<WorkspaceInfo, ()>,
    // Shelf Management
    pub shelves: StatefulMap<ShelfId, ImmutRef<Shelf<TagFilter>>>,
    // Tag Management
    pub tags: StatefulMap<TagId, SharedRef<Tag>>,
    pub lookup: StatefulMap<String, TagId>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceInfo {
    pub name: Stamped<String>,
    pub description: Stamped<String>,
}

// Values only 
impl PartialEq for WorkspaceInfo {
    fn eq(&self, other: &Self) -> bool {
        self.name.get() == other.name.get() && self.description.get() == other.description.get()
    }
}

impl Eq for WorkspaceInfo {}

impl WorkspaceInfo {
    pub fn new(name: Option<String>, description: Option<String>) -> Self {
        let at = Utc::now();
        WorkspaceInfo {
            name: Stamped::new(name.unwrap_or_else(|| "Workspace".to_string()), at),
            description: Stamped::new(description.unwrap_or_default(), at),
        }
    }

    pub fn edited(&self, name: String, description: String, at: Revision) -> Self {
        WorkspaceInfo {
            name: self.name.set(name, at),
            description: self.description.set(description, at),
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        WorkspaceInfo {
            name: self.name.merge(&other.name),
            description: self.description.merge(&other.description),
        }
    }
}

impl<TagFilter> Clone for Workspace<TagFilter> {
    fn clone(&self) -> Self {
        Workspace {
            info: self.info.clone_inner(),
            shelves: self.shelves.clone(),
            tags: self.tags.clone(),
            lookup: self.lookup.clone(),
        }
    }
}

#[derive(Debug)]
pub enum TagErr {
    TagMissing(Vec<TagId>),
    ParentMissing(TagId),
    DuplicateTag((String, WorkspaceId)),
    InconsistentTagManager((TagId, WorkspaceId)),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration};

    fn at(secs: i64) -> Revision {
        DateTime::<Utc>::UNIX_EPOCH + Duration::seconds(secs)
    }

    fn info(name: &str, description: &str, base: i64) -> WorkspaceInfo {
        WorkspaceInfo {
            name: Stamped::new(name.to_string(), at(base)),
            description: Stamped::new(description.to_string(), at(base)),
        }
    }

    // Future behaviour being equal requires equal timestamps, but `PartialEq` only compares values
    fn full(i: &WorkspaceInfo) -> (String, String, Revision, Revision) {
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
}
