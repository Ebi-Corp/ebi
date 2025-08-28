use crate::sharedref::{SharedRef, ImmutRef};
use crate::shelf::ShelfOwner;
use crate::tag::Tag;
use chrono::{DateTime, Utc};
use papaya::HashSet;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::PathBuf;
pub type FileRef = SharedRef<File>;
pub type TagRef = ImmutRef<Tag>;

#[derive(Debug, Clone)]
pub struct File {
    pub path: PathBuf,
    pub hash: u64,
    pub metadata: FileMetadata,
    pub tags: HashSet<TagRef>,
    pub dtags: HashSet<TagRef>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSummary {
    pub owner: ShelfOwner,
    pub path: PathBuf,
    pub metadata: FileMetadata,
    //[?] Icon/Preview ??
}

impl FileSummary {
    fn new(owner: ShelfOwner, path: PathBuf, metadata: FileMetadata) -> Self {
        FileSummary {
            owner,
            path,
            metadata,
        }
    }
    pub fn from(file: &File, shelf: ShelfOwner) -> Self {
        FileSummary::new(shelf, file.path.clone(), file.metadata.clone())
    }
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

impl File {
    pub fn new(
        path: PathBuf,
        tags: HashSet<TagRef>,
        dtags: HashSet<TagRef>,
        metadata: FileMetadata,
    ) -> Self {
        File {
            path,
            hash: 0,
            metadata,
            tags,
            dtags,
        }
    }

    pub fn attach(&self, tag: &TagRef) -> bool {
        self.tags.pin().insert(tag.clone())
    }

    pub fn detach(&self, tag: &TagRef) -> bool {
        self.tags.pin().remove(tag)
    }

    pub fn attach_dtag(&self, tag: &TagRef) -> bool {
        self.dtags.pin().insert(tag.clone())
    }

    pub fn detach_dtag(&self, tag: &TagRef) -> bool {
        self.dtags.pin().remove(tag)
    }
}
