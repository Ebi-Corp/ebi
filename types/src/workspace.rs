use crate::Uuid;
use crate::shelf::{Shelf, ShelfId};
use crate::tag::{Tag, TagId};
use crate::{ImmutRef, InfoState, SharedRef, StatefulField, StatefulMap, StatefulRef};

pub type WorkspaceId = Uuid;

#[derive(Debug)]
pub struct Workspace<TagFilter> {
    // Workspace Info
    pub info: StatefulRef<WorkspaceInfo, ()>,
    // Shelf Management
    pub shelves: StatefulMap<ShelfId, ImmutRef<Shelf<TagFilter>>>,
    // Tag Management
    pub tags: StatefulMap<TagId, SharedRef<Tag>>,
    pub lookup: StatefulMap<String, TagId>,
}

#[derive(Debug)]
pub struct WorkspaceInfo {
    pub name: StatefulField<WorkspaceInfoField, String>,
    pub description: StatefulField<WorkspaceInfoField, String>,
}

impl PartialEq for WorkspaceInfo {
    fn eq(&self, other: &Self) -> bool {
        self.name.get() == other.name.get() && self.description.get() == other.description.get()
    }
}

impl Eq for WorkspaceInfo {}

impl WorkspaceInfo {
    pub fn new(name: Option<String>, description: Option<String>) -> Self {
        let default_name = "Workspace".to_string();
        let default_description = "".to_string();
        let name = name.unwrap_or(default_name);
        let description = description.unwrap_or(default_description);
        let info_state: InfoState<WorkspaceInfoField> = InfoState::new();
        WorkspaceInfo {
            name: {
                let field = StatefulField::<WorkspaceInfoField, String>::new(
                    WorkspaceInfoField::Name,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&name);
                drop(updater); // No State Update required for Info Creation
                field
            },
            description: {
                let field = StatefulField::<WorkspaceInfoField, String>::new(
                    WorkspaceInfoField::Description,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&description);
                drop(updater); // No State Update required for Info Creation
                field
            },
        }
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum WorkspaceInfoField {
    Name,
    Description,
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
