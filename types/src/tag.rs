use crate::stateful::Revision;
use crate::{SharedRef, Stamped};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use std::hash::Hash;

pub type TagId = crate::Uuid;
pub type TagRef = SharedRef<Tag>;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TagData {
    pub name: String,
    pub priority: u64,
    pub parent: Option<TagId> // Permanent ID of the parent, if any
}

impl From<&Tag> for TagData {
    fn from(tag: &Tag) -> Self {
        TagData {
            name: tag.name.get().clone(),
            priority: *tag.priority.get(),
            parent: *tag.parent.get(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Tag {
    pub priority: Stamped<u64>,
    pub name: Stamped<String>,
    pub parent: Stamped<Option<TagId>>,
    //[+] pub visible: bool, // Whether the tag is visible in the UI
}

impl Tag {
    pub fn created(name: String, priority: u64, parent: Option<TagId>, at: Revision) -> Self {
        Tag {
            name: Stamped::new(name, at),
            priority: Stamped::new(priority, at),
            parent: Stamped::new(parent, at),
        }
    }

    pub fn edited(&self, name: String, priority: u64, parent: Option<TagId>, at: Revision) -> Self {
        Tag {
            name: self.name.set(name, at),
            priority: self.priority.set(priority, at),
            parent: self.parent.set(parent, at),
        }
    }

    pub fn merge(&self, other: &Tag) -> Self {
        Tag {
            name: self.name.merge(&other.name),
            priority: self.priority.merge(&other.priority),
            parent: self.parent.merge(&other.parent),
        }
    }
}
impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Tag", 3)?;

        state.serialize_field("priority", self.priority.get())?;
        state.serialize_field("name", self.name.get())?;

        state.serialize_field("parent_id", self.parent.get())?;

        state.end()
    }
}
