use crate::file::{File, FileRef};
use arc_swap::ArcSwap;
use ebi_proto::rpc::ReturnCode;
use ebi_types::tag::TagRef;
use ebi_types::{FileId, ImmutRef, Ref, WeakRef, WithPath, get_file_id};
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
    pub path: PathBuf,
    pub files: HashSet<FileRef>,
    pub collector: Arc<Collector>,
    pub tags: HashMap<TagRef, HashSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // dtags applied from above, to be applied down
    pub dtag_dirs: HashMap<TagRef, Vec<ShelfDirRef>>, // list of dtagged directories starting at any point below
    pub parent: ParentRef,
    pub subdirs: HashSet<ShelfDirRef>,
}

impl WithPath for ShelfDir {
    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl ShelfDir {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let collector = Arc::new(Collector::new());
        Ok(ShelfDir {
            files: hash_set!(collector),
            path,
            collector: collector.clone(),
            tags: hash_map!(collector),
            dtags: hash_set!(collector),
            dtag_dirs: hash_map!(collector),
            parent: ArcSwap::new(Arc::new(None)),
            subdirs: hash_set!(collector),
        })
    }
    pub(crate) fn get_init_file(&self, path: PathBuf) -> Result<(FileRef, bool), ReturnCode> {
        match self.files.pin().iter().find(|s| *s.path() == path) {
            Some(file) => Ok((file.clone(), false)),
            None => {
                let file_id = get_file_id(&path).map_err(|_| ReturnCode::FileNotFound)?;
                let file = File::new(
                    path,
                    papaya::HashSet::<TagRef>::builder()
                        .shared_collector(self.collector.clone())
                        .build(),
                );
                let file = ImmutRef::new_ref_id(file_id, file);
                self.files.pin().insert(file.clone());
                Ok((file, true))
            }
        }
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
                    if set.is_empty() {
                        self.tags.pin().remove(tag);
                    }
                    res
                } else {
                    false
                }
            }
            None => self.tags.pin().remove(tag).is_some(),
        }
    }
}
