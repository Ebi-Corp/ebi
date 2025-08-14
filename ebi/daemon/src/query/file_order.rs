use crate::shelf::file::FileSummary;
use ebi_proto::rpc::{OrderBy, ReturnCode};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedFileSummary {
    pub file_summary: FileSummary,
    pub order: FileOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileOrder {
    Name,
    Size,
    Created,
    Modified,
    Accessed,
    Unordered,
}

impl Hash for OrderedFileSummary {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_summary.path.hash(state);
        self.file_summary.owner.hash(state);
    }
}

impl PartialEq for OrderedFileSummary {
    fn eq(&self, other: &Self) -> bool {
        if self.file_summary.owner != other.file_summary.owner {
            return false;
        }
        // [!] this should be changed to hash
        self.file_summary.path.file_name() == other.file_summary.path.file_name()
            && self.file_summary.metadata.size == other.file_summary.metadata.size
    }
}
impl Eq for OrderedFileSummary {}

impl PartialOrd for OrderedFileSummary {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFileSummary {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.order {
            FileOrder::Name => self
                .file_summary
                .path
                .file_name()
                .unwrap()
                .cmp(other.file_summary.path.file_name().unwrap()),
            FileOrder::Created => self
                .file_summary
                .metadata
                .created
                .cmp(&other.file_summary.metadata.created),
            FileOrder::Modified => self
                .file_summary
                .metadata
                .modified
                .cmp(&other.file_summary.metadata.modified),
            FileOrder::Accessed => self
                .file_summary
                .metadata
                .accessed
                .cmp(&other.file_summary.metadata.accessed),
            FileOrder::Size => self
                .file_summary
                .metadata
                .size
                .cmp(&other.file_summary.metadata.size),

            FileOrder::Unordered => {
                todo!();
            }
        }
    }
}

impl From<OrderBy> for FileOrder {
    fn from(proto_order: OrderBy) -> Self {
        match proto_order {
            OrderBy::Name => FileOrder::Name,
            OrderBy::Size => FileOrder::Size,
            OrderBy::Modified => FileOrder::Modified,
            OrderBy::Accessed => FileOrder::Accessed,
            OrderBy::Created => FileOrder::Created,
            OrderBy::Unordered => FileOrder::Unordered,
        }
    }
}

impl TryFrom<i32> for FileOrder {
    type Error = ReturnCode;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FileOrder::Name),
            1 => Ok(FileOrder::Size),
            2 => Ok(FileOrder::Modified),
            3 => Ok(FileOrder::Accessed),
            4 => Ok(FileOrder::Created),
            5 => Ok(FileOrder::Unordered),
            _ => Err(ReturnCode::ParseError),
        }
    }
}
