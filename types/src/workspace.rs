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
