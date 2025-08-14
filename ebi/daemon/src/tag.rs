use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

pub type TagId = Uuid;

#[derive(Clone)]
pub struct TagData {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Box<TagData>>,
}

#[derive(Debug, Eq, PartialEq, Hash, Default)]
pub struct Tag {
    pub id: TagId,
    pub priority: u64,
    pub name: String,
    pub parent: Option<TagRef>,
    //[+] pub visible: bool, // Whether the tag is visible in the UI
}

#[derive(Debug)]
pub struct TagRef {
    pub tag_ref: Arc<RwLock<Tag>>,
}

impl Hash for TagRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag_ref.read().unwrap().id.hash(state);
    }
}

impl Clone for TagRef {
    fn clone(&self) -> Self {
        TagRef {
            tag_ref: Arc::clone(&self.tag_ref),
        }
    }
}

impl PartialEq for TagRef {
    fn eq(&self, other: &Self) -> bool {
        self.tag_ref.read().unwrap().id == other.tag_ref.read().unwrap().id
    }
}

impl Eq for TagRef {}
