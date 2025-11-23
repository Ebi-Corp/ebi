macro_rules! hash_set {
    ($collector:expr) => {
        papaya::HashSet::builder()
            .shared_collector($collector.clone())
            .build()
    };
}
macro_rules! hash_map {
    ($collector:expr) => {
        papaya::HashMap::builder()
            .shared_collector($collector.clone())
            .build()
    };
}

pub mod dir;
pub mod file;
pub mod redb;
pub mod service;
pub mod shelf;
use crate::dir::ShelfDir;
use crate::file::File;
use crate::shelf::ShelfData;
use ebi_types::{FileId, ImmutRef, Inner, Ref};
use std::sync::Arc;

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

impl Ref<ShelfData, FileId> for ImmutRef<ShelfData, FileId> {
    fn new_ref(data: ShelfData) -> Self {
        Inner::new(data.root.id, Arc::new(data))
    }

    fn new_ref_id(id: FileId, data: ShelfData) -> Self {
        Inner::new(id, Arc::new(data))
    }

    fn inner_ptr(&self) -> *const ShelfData {
        Arc::as_ptr(self.data_ref())
    }
}
