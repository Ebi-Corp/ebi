use crate::dir::{HashSet, ShelfDir, ShelfDirRef};
use crate::file::FileRef;
use ebi_types::tag::{Tag, TagId};
use ebi_types::{FileId, ImmutRef, SharedRef, Uuid, WithPath};
use rand_chacha::{ChaCha12Rng, rand_core::SeedableRng};
use scalable_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use seize::Collector;
use std::io;
use std::path::PathBuf;
use std::result::Result;
use std::sync::Arc;

pub type ShelfId = Uuid;
pub type TagRef = SharedRef<Tag>;

const SEED: u64 = 0; // [TODO] Move seed to proper initialization

pub type ShelfDataRef = ImmutRef<ShelfData>;
#[derive(Debug, Clone)]
pub struct TagFilter(
    pub ScalableCuckooFilter<TagId, scalable_cuckoo_filter::DefaultHasher, ChaCha12Rng>,
);

impl Default for TagFilter {
    fn default() -> Self {
        let builder = ScalableCuckooFilterBuilder::new();
        let rng = ChaCha12Rng::seed_from_u64(SEED);
        let builder = builder.rng(rng);
        TagFilter(builder.finish::<TagId>())
    }
}

#[derive(Debug)]
pub struct ShelfData {
    pub root: ImmutRef<ShelfDir, FileId>,
    pub dirs: HashSet<ShelfDirRef>,
    pub root_path: PathBuf,
}

impl PartialEq for ShelfData {
    fn eq(&self, other: &Self) -> bool {
        self.root_path == other.root_path
    }
}

impl WithPath for ShelfData {
    fn path(&self) -> &PathBuf {
        &self.root_path
    }
}

impl ShelfData {
    pub fn new(root_ref: ImmutRef<ShelfDir, FileId>) -> Result<Self, io::Error> {
        let path = root_ref.path.clone();
        let collector = Arc::new(Collector::new());
        let dirs = papaya::HashSet::<ShelfDirRef>::builder()
            .shared_collector(collector)
            .build();
        let downgraded_root = root_ref.downgrade();
        dirs.pin().insert(downgraded_root);
        Ok(ShelfData {
            root: root_ref,
            dirs,
            root_path: path,
        })
    }

    pub async fn files(&self) -> Vec<FileRef> {
        self.root.files.pin().iter().cloned().collect()
    }

    pub fn contains(&self, tag: TagRef) -> bool {
        self.root.tags.pin().contains_key(&tag) || self.root.dtag_dirs.pin().contains_key(&tag)
    }
}

#[derive(Debug)]
pub enum UpdateErr {
    PathNotFound,
    FileNotFound,
    PathNotDir,
}

pub fn merge<T: Clone + std::cmp::Eq + std::hash::Hash>(files: Vec<im::HashSet<T>>) -> Vec<T> {
    //[?] Is the tournament-style Merge-Sort approach the most efficient method ??
    //[/] BTreeSets are not guaranteed to be the same size
    //[TODO] Time & Space Complexity analysis

    let mut final_res = Vec::<T>::new();
    let mut chunks = files.into_iter();

    // processing into two chunks
    while let Some(a) = chunks.next() {
        let mut to_append: Vec<T> = {
            if let Some(b) = chunks.next() {
                // Merging the smaller set into the larger one is more efficient
                let (mut larger, smaller) = if a.len() <= b.len() { (a, b) } else { (b, a) };
                larger.extend(smaller);
                larger.into_iter().collect()
            } else {
                a.into_iter().collect()
            }
        };
        final_res.append(&mut to_append); // remainder chunk
    }
    final_res
}

#[cfg(test)] //[!] Check 
mod tests {
    use crate::file::File;
    use ebi_types::Ref;
    use jwalk::WalkDir;
    use rayon::prelude::*;
    use seize::Collector;
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    fn list_files(root: PathBuf) -> Vec<FileRef> {
        WalkDir::new(root)
            .into_iter()
            .filter_map(|entry_res| {
                let entry = entry_res.unwrap();
                if entry.file_type().is_file() {
                    Some(FileRef::new_ref(File::new(
                        entry.path().clone(),
                        papaya::HashSet::builder()
                            .shared_collector(Arc::new(Collector::new()))
                            .build(),
                    )))
                } else {
                    None
                }
            })
            .collect()
    }
}
