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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};

    fn at(secs: i64) -> Revision {
        DateTime::<Utc>::UNIX_EPOCH + chrono::Duration::seconds(secs)
    }

    fn tag(name: &str, priority: u64, base: i64) -> Tag {
        Tag {
            name: Stamped::new(name.to_string(), at(base)),
            priority: Stamped::new(priority, at(base)),
            parent: Stamped::new(None, at(base)),
        }
    }

    #[test]
    fn tag_field_independence() {
        let original = tag("first", 1, 10);

        let renamed = original.edited("second".to_string(), 1, None, at(20));
        assert_eq!(renamed.name.rev(), at(20));
        assert_eq!(renamed.priority.rev(), at(10));
        assert_eq!(renamed.parent.rev(), at(10));

        let no_op = renamed.edited("second".to_string(), 1, None, at(30));
        assert_eq!(no_op.name.rev(), renamed.name.rev());
        assert_eq!(no_op.priority.rev(), renamed.priority.rev());
        assert_eq!(no_op.parent.rev(), renamed.parent.rev());
    }

    #[test]
    fn merge_concurrent_edits() {
        let renamed = tag("first", 1, 10).edited("renamed".to_string(), 1, None, at(20));
        let reprioritised = tag("first", 1, 10).edited("first".to_string(), 9, None, at(21));

        let merged = renamed.merge(&reprioritised);
        assert_eq!(merged.name.get(), "renamed");
        assert_eq!(*merged.priority.get(), 9);
        assert_eq!(merged.name.rev(), at(20));
        assert_eq!(merged.priority.rev(), at(21));
    }

    #[test]
    fn merge_idempotent() {
        let a = tag("a", 1, 10).edited("a2".to_string(), 1, None, at(20));
        assert_eq!(a.merge(&a), a);
        assert_eq!(a.merge(&a).merge(&a), a);
    }

    #[test]
    fn merge_commutative() {
        let a = tag("a", 1, 10).edited("a".to_string(), 1, None, at(20));
        let b = tag("b", 9, 10).edited("b".to_string(), 9, None, at(21));
        assert_eq!(a.merge(&b), b.merge(&a));
    }

    #[test]
    fn merge_associative() {
        let a = tag("a", 1, 10).edited("a".to_string(), 1, None, at(20));
        let b = tag("b", 9, 10).edited("b".to_string(), 9, None, at(21));
        let c = tag("c", 5, 10).edited("c".to_string(), 5, None, at(25));

        assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
        assert_eq!(c.merge(&a).merge(&b), b.merge(&c.merge(&a)));
    }
}
