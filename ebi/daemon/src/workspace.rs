use crate::shelf::file::FileRef;
use crate::shelf::{Shelf, ShelfId};
use crate::stateful::StatefulMap;
use crate::sharedref::ImmutRef;
use crate::tag::{Tag, TagId};
use arc_swap::ArcSwap;
use serde::Serialize;
use uuid::Uuid;

pub type WorkspaceId = Uuid;

#[derive(Debug)]
pub struct Workspace {
    // Workspace Info
    pub info: ArcSwap<WorkspaceInfo>,
    // Shelf Management
    pub shelves: StatefulMap<ShelfId, ImmutRef<Shelf>>,
    // Tag Management
    pub tags: StatefulMap<TagId, ImmutRef<Tag>>,
    pub lookup: StatefulMap<String, TagId>,
}


#[derive(Debug, Clone, Serialize)]
pub struct WorkspaceInfo {
    pub name: String,
    pub description: String,
}

impl Clone for Workspace {
    fn clone(&self) -> Self {
        Workspace {
            info: ArcSwap::new(self.info.load_full()),
            shelves: self.shelves.clone(),
            tags: self.tags.clone(),
            lookup: self.lookup.clone()
        }
    }
}

impl Workspace {
    pub async fn refresh(&mut self, _shelf: ShelfId, _change: ChangeSummary) {
        todo!();
    }
}

pub struct ChangeSummary {
    pub added_files: Vec<FileRef>,
    pub removed_files: Vec<FileRef>,
    pub modified_files: Vec<FileRef>,
}

#[derive(Debug)]
pub enum TagErr {
    TagMissing(Vec<TagId>),
    ParentMissing(TagId),
    DuplicateTag((String, WorkspaceId)),
    InconsistentTagManager((TagId, WorkspaceId)),
}
