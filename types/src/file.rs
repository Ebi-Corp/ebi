use crate::FileId;
use crate::shelf::ShelfOwner;
use crate::tag::TagData;
use chrono::{DateTime, Utc};
use ebi_proto::rpc::{OrderBy, ReturnCode};
use im::HashSet;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::PathBuf;
use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSummary {
    pub id: FileId,
    pub owner: Option<ShelfOwner>,
    pub path: PathBuf,
    pub tags: HashSet<TagData>,
    pub metadata: FileMetadata,
    //[?] Icon/Preview ??
}

impl FileSummary {
    pub fn new(
        id: FileId,
        path: PathBuf,
        owner: Option<ShelfOwner>,
        tags: HashSet<TagData>,
    ) -> Self {
        let metadata = FileMetadata::new(&path);
        FileSummary {
            id,
            owner,
            path,
            tags,
            metadata,
        }
    }
    /*
    pub fn from(file: &FileRef, shelf: Option<ShelfOwner>) -> Self {
        FileSummary::new(
            file.id,
            file.path.clone(),
            shelf,
            file.tags
                .pin()
                .iter()
                .map(|t| ebi_types::tag::TagData::from(&*t.load_full()))
                .collect(),
        )
    }
    */
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub size: u64,
    pub readonly: bool,
    pub modified: Option<DateTime<Utc>>,
    pub accessed: Option<DateTime<Utc>>,
    pub created: Option<DateTime<Utc>>,
    pub unix: Option<UnixMetadata>,
    pub windows: Option<WindowsMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnixMetadata {
    pub permissions: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowsMetadata {
    pub attributes: u32,
}

impl FileMetadata {
    pub fn new(path: &PathBuf) -> Self {
        let meta = match std::fs::metadata(path) {
            Ok(meta) => meta,
            Err(_) => {
                return FileMetadata {
                    size: 0,
                    readonly: false,
                    modified: None,
                    accessed: None,
                    created: None,
                    unix: None,
                    windows: None,
                };
            }
        };

        FileMetadata {
            size: meta.len(),
            readonly: meta.permissions().readonly(),
            modified: meta.modified().ok().map(DateTime::<Utc>::from),
            accessed: meta.accessed().ok().map(DateTime::<Utc>::from),
            created: meta.created().ok().map(DateTime::<Utc>::from),

            #[cfg(unix)]
            unix: Some(UnixMetadata {
                permissions: meta.mode(),
                uid: meta.uid(),
                gid: meta.gid(),
            }),
            #[cfg(windows)]
            unix: None,

            #[cfg(windows)]
            windows: Some(WindowsMetadata {
                attributes: meta.file_attributes(),
            }),
            #[cfg(unix)]
            windows: None,
        }
    }
}

impl From<FileMetadata> for ebi_proto::rpc::FileMetadata {
    fn from(f_meta: FileMetadata) -> Self {
        fn encode_timestamp(opt_dt: Option<DateTime<Utc>>) -> Option<i64> {
            opt_dt.map_or_else(|| None, |dt| Some(dt.timestamp()))
        }
        let os_metadata = match (f_meta.unix, f_meta.windows) {
            (Some(unix), None) => {
                ebi_proto::rpc::file_metadata::OsMetadata::Unix(ebi_proto::rpc::UnixMetadata {
                    permissions: unix.permissions,
                    uid: unix.uid,
                    gid: unix.gid,
                })
            }
            (None, Some(windows)) => ebi_proto::rpc::file_metadata::OsMetadata::Windows(
                ebi_proto::rpc::WindowsMetadata {
                    attributes: windows.attributes,
                },
            ),
            _ => ebi_proto::rpc::file_metadata::OsMetadata::Error(ebi_proto::rpc::Empty {}),
        };

        ebi_proto::rpc::FileMetadata {
            size: f_meta.size,
            readonly: f_meta.readonly,
            modified: encode_timestamp(f_meta.modified),
            accessed: encode_timestamp(f_meta.accessed),
            created: encode_timestamp(f_meta.created),
            os_metadata: Some(os_metadata),
        }
    }
}

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

impl Borrow<FileId> for OrderedFileSummary {
    fn borrow(&self) -> &FileId {
        &self.file_summary.id
    }
}

impl Hash for OrderedFileSummary {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_summary.path.hash(state);
        self.file_summary.owner.hash(state);
    }
}

impl PartialEq for OrderedFileSummary {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.id == other.file_summary.id
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
