use ebi_types::{
    FileId, ImmutRef, SharedRef, WithPath, file::FileSummary, shelf::ShelfOwner, tag::Tag,
    tag::TagData,
};
use im::HashSet;
use std::path::PathBuf;

use crate::ModifiablePath;

pub type FileRef = ImmutRef<File, FileId>;
pub type TagRef = SharedRef<Tag>;

#[derive(Debug)]
pub struct File {
    pub path: ModifiablePath,
    pub tags: crate::dir::HashSet<TagRef>,
}

impl WithPath for File {
    fn path(&self) -> PathBuf {
        self.path.0.load_full().as_ref().clone()
    }
}

pub fn gen_summary(
    file_ref: &FileRef,
    owner: Option<ShelfOwner>,
    dtags: HashSet<TagData>,
) -> FileSummary {
    let mut tags: HashSet<TagData> = file_ref
        .tags
        .pin()
        .iter()
        .map(|t| TagData::from(&*t.load_full()))
        .collect();
    tags = tags.union(dtags);
    FileSummary::new(file_ref.id, file_ref.path(), owner, tags)
}

impl File {
    pub fn new(path: PathBuf, tags: crate::dir::HashSet<TagRef>) -> Self {
        File {
            path: ModifiablePath::new(path),
            tags,
        }
    }

    pub fn attach(&self, tag: &TagRef) -> bool {
        self.tags.pin().insert(tag.clone())
    }

    pub fn detach(&self, tag: &TagRef) -> bool {
        self.tags.pin().remove(tag)
    }
}
