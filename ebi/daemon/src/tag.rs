use crate::prelude::*;
use serde::{Serialize, Serializer, ser::SerializeStruct};
use std::hash::Hash;

pub type TagId = Uuid;
pub type TagRef = SharedRef<Tag>;

#[derive(Clone)]
pub struct TagData {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Box<TagData>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct Tag {
    pub priority: u64,
    pub name: String,
    pub parent: Option<TagRef>,
    //[+] pub visible: bool, // Whether the tag is visible in the UI
}
impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Tag", 3)?;

        state.serialize_field("priority", &self.priority)?;
        state.serialize_field("name", &self.name)?;

        // Serialize parent ID instead of the full reference
        let parent_id = self.parent.as_ref().map(|p| p.id); // Assuming SharedRef has an id field
        state.serialize_field("parent_id", &parent_id)?;

        state.end()
    }
}
