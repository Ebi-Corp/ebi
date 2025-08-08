use crate::shelf::file::FileRef;
use crate::shelf::{Shelf, ShelfId};
use crate::tag::{Tag, TagData, TagId, TagRef};
use ebi_proto::rpc::ReturnCode;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub type WorkspaceId = Uuid;

pub type WorkspaceRef = Arc<RwLock<Workspace>>;

//[#] Workspace calls Shelf.refresh(), Shelf returns a ChangeSummary, Workspace applies Autotaggers

pub struct Workspace {
    // Workspace Info
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    // Shelf Management
    pub shelves: HashMap<ShelfId, Arc<RwLock<Shelf>>>,
    // Tag Management
    pub tags: HashMap<TagId, TagRef>,
    pub lookup: HashMap<String, TagId>,
}

impl Workspace {
    pub fn get_tags(&mut self) -> Vec<TagRef> {
        self.tags.values().cloned().collect()
    }

    // [!] Check if Tag with same name exists
    pub fn create_tag(
        &mut self,
        priority: u64,
        name: String,
        parent: Option<TagRef>,
    ) -> Result<TagId, ReturnCode> {
        let id = Uuid::now_v7();
        if self.lookup.contains_key(&name) {
            return Err(ReturnCode::TagNameDuplicate);
        }
        let tag = Tag {
            id,
            priority,
            name: name.clone(),
            parent,
        };
        self.lookup.insert(name, id);
        self.tags.insert(
            id,
            TagRef {
                tag_ref: Arc::new(std::sync::RwLock::new(tag)),
            },
        );
        Ok(id)
    }

    pub fn get_tag(&mut self, name: &str) -> Result<TagRef, TagErr> {
        let id = self.lookup.get(name);
        if let Some(id) = id {
            let tag_ref = self.tags.get(id).cloned();
            Ok(tag_ref.unwrap())
        } else {
            Err(TagErr::TagMissing(Vec::new()))
        }
    }

    pub async fn refresh(&mut self, _shelf: ShelfId, _change: ChangeSummary) {
        todo!();
    }

    // Private function to be used in refresh
    fn get_or_create(&mut self, tag_data: TagData) -> TagRef {
        match self.lookup.get(&tag_data.name) {
            Some(tag_id) => self.tags.get(tag_id).unwrap().clone(),
            None => {
                let tag_id = self
                    .create_tag(tag_data.priority, tag_data.name, None)
                    .unwrap();
                self.tags.get(&tag_id).unwrap().clone()
            }
        }
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
