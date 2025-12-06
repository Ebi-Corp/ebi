use crate::ModifiablePath;
use crate::file::FileRef;
use arc_swap::ArcSwap;
use ebi_types::tag::TagRef;
use ebi_types::{FileId, WeakRef, WithPath};
use seize::Collector;
use std::hash::RandomState;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

pub type ShelfDirRef = WeakRef<ShelfDir, FileId>;
pub type ParentRef = ArcSwap<Option<ShelfDirRef>>;
pub(crate) type HashSet<T> = papaya::HashSet<T, RandomState, Arc<Collector>>;
pub(crate) type HashMap<K, V> = papaya::HashMap<K, V, RandomState, Arc<Collector>>;

#[derive(Debug)]
pub struct ShelfDir {
    pub path: ModifiablePath,
    pub files: HashSet<FileRef>,
    pub collector: Arc<Collector>,
    pub tags: HashMap<TagRef, HashSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // dtags applied from above, to be applied down
    pub dtag_dirs: HashMap<TagRef, Vec<ShelfDirRef>>, // list of dtagged directories starting at any point below
    pub parent: ParentRef,
    pub subdirs: HashSet<ShelfDirRef>,
}

impl WithPath for ShelfDir {
    fn path(&self) -> PathBuf {
        self.path.0.load_full().as_ref().clone()
    }
}

impl ShelfDir {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let collector = Arc::new(Collector::new());
        Ok(ShelfDir {
            files: hash_set!(collector),
            path: ModifiablePath::new(path),
            collector: collector.clone(),
            tags: hash_map!(collector),
            dtags: hash_set!(collector),
            dtag_dirs: hash_map!(collector),
            parent: ArcSwap::new(Arc::new(None)),
            subdirs: hash_set!(collector),
        })
    }

    pub fn attach(&self, tag: &TagRef, file: &FileRef) -> bool {
        let tags = self.tags.pin();
        let existed = tags.get(tag).is_some();

        if let Some(set) = tags.get(tag) {
            set.pin().insert(file.clone());
        } else {
            let new_set = HashSet::default();
            new_set.pin().insert(file.clone());
            tags.insert(tag.clone(), new_set);
        }

        !existed
    }

    pub fn detach(&self, tag: &TagRef, file: Option<&FileRef>) -> bool {
        match file {
            Some(file) => {
                if let Some(set) = self.tags.pin().get(tag) {
                    let res = set.pin().remove(file);
                    if res && set.is_empty() {
                        self.tags.pin().remove(tag);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            None => self.tags.pin().remove(tag).is_some(),
        }
    }
}
