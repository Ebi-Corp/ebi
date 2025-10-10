pub mod dir;
pub mod shelf;
pub mod service;
pub mod file;
pub use file_id::FileId;
use crate::file::File;
use crate::dir::ShelfDir;
use std::sync::Arc;
use ebi_types::{ImmutRef, Ref, Inner};


impl Ref<File, FileId> for ImmutRef<File, FileId> {
    fn new_ref(_data: File) -> Self {
        unimplemented!("Use new_ref_id instead");
    }

    fn new_ref_id(id: FileId, data: File) -> Self {
        Inner::new(id, Arc::new(data))
    }
    fn inner_ptr(&self) -> *const File {
        Arc::as_ptr(self.data_ref())
    }
}

impl Ref<ShelfDir, FileId> for ImmutRef<ShelfDir, FileId> {
    fn new_ref(_data: ShelfDir) -> Self {
        unimplemented!("Use new_ref_id instead");
    }

    fn new_ref_id(id: FileId, data: ShelfDir) -> Self {
        Inner::new(id, Arc::new(data))
    }

    fn inner_ptr(&self) -> *const ShelfDir {
        Arc::as_ptr(self.data_ref())
    }
}
