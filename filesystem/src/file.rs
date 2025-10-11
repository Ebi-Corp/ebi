use ebi_types::{
    ImmutRef, SharedRef, WithPath, file::FileSummary, shelf::ShelfOwner, tag::Tag, tag::TagData,
};
use file_id::FileId;
use std::path::PathBuf;

pub type FileRef = ImmutRef<File, FileId>;
pub type TagRef = SharedRef<Tag>;

#[derive(Debug)]
pub struct File {
    pub path: PathBuf,
    pub tags: crate::dir::HashSet<TagRef>,
}

impl WithPath for File {
    fn path(&self) -> &PathBuf {
        &self.path
    }
}

pub fn gen_summary(file_ref: &FileRef, owner: Option<ShelfOwner>) -> FileSummary {
    FileSummary::new(
        file_ref.id,
        file_ref.path.clone(),
        owner,
        file_ref
            .tags
            .pin()
            .iter()
            .map(|t| TagData::from(&*t.load_full()))
            .collect(),
    )
}

impl File {
    pub fn new(path: PathBuf, tags: crate::dir::HashSet<TagRef>) -> Self {
        File { path, tags }
    }

    pub fn attach(&self, tag: &TagRef) -> bool {
        self.tags.pin().insert(tag.clone())
    }

    pub fn detach(&self, tag: &TagRef) -> bool {
        self.tags.pin().remove(tag)
    }
}
