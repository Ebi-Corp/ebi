pub mod dir;
pub mod file;
use crate::prelude::*;
use crate::sharedref::ptr_eq;
use crate::shelf::dir::{HashSet, ShelfDir, ShelfDirRef};
use crate::shelf::file::File;
use crate::stateful::{InfoState, StatefulField};
use crate::tag::{Tag, TagId};
use arc_swap::ArcSwap;
use chrono::Duration;
use file::FileRef;
use file_id::{FileId, get_file_id};
use iroh::NodeId;
use rand_chacha::{ChaCha12Rng, rand_core::SeedableRng};
use scalable_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use seize::Collector;
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::result::Result;
use tokio::sync::RwLock;

pub type ShelfId = Uuid;
pub type TagRef = SharedRef<Tag>;

const SEED: u64 = 0; // [TODO] Move seed to proper initialization

pub type ShelfDataRef = ImmutRef<ShelfData>;
pub type TagFilter =
    ScalableCuckooFilter<TagId, scalable_cuckoo_filter::DefaultHasher, ChaCha12Rng>;

#[derive(Clone)]
pub enum ShelfType {
    Local(ShelfDataRef),
    Remote,
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum ShelfOwner {
    Node(NodeId),
    Sync(SyncId),
}

//[#] Sync

pub type SyncId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    //[!] Placeholder for sync configuration
    pub interval: Option<Duration>, // Auto-Sync Interval
    pub auto_sync: bool,            // Auto-Sync on changes
}

//[#] Sync

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShelfConfig {
    //[TODO] Define a configuration for the shelf
    pub sync_config: Option<SyncConfig>,
}

pub struct Shelf {
    pub shelf_type: ShelfType,
    pub shelf_owner: ShelfOwner,
    pub config: ShelfConfig,
    pub filter_tags: ArcSwap<TagFilter>,
    pub info: StatefulRef<ShelfInfo>,
}

impl Clone for Shelf {
    fn clone(&self) -> Self {
        Shelf {
            shelf_type: self.shelf_type.clone(),
            shelf_owner: self.shelf_owner.clone(),
            config: self.config.clone(),
            filter_tags: ArcSwap::new(self.filter_tags.load_full()),
            info: self.info.clone(),
        }
    }
}

impl Shelf {
    pub fn new(
        lock: Arc<RwLock<()>>,
        path: PathBuf,
        root_ref: Option<ImmutRef<ShelfDir, FileId>>,
        name: String,
        shelf_owner: ShelfOwner,
        config: Option<ShelfConfig>,
        description: String,
    ) -> Result<Shelf, io::Error> {
        let shelf_type = if let Some(root_ref) = root_ref {
            let shelf_data = ShelfData::new(root_ref)?;
            ShelfType::Local(ImmutRef::new_ref(shelf_data))
        } else {
            ShelfType::Remote
        };
        let shelf = Shelf {
            shelf_type,
            shelf_owner,
            config: config.unwrap_or_default(),
            filter_tags: generate_tag_filter(), // [TODO] Filter parameters (size, ...) should be configurable
            info: StatefulRef::new_ref(ShelfInfo::new(Some(name), Some(description), path), lock),
        };
        Ok(shelf)
    }
}

pub fn generate_tag_filter() -> ArcSwap<TagFilter> {
    let builder = ScalableCuckooFilterBuilder::new();
    let rng = ChaCha12Rng::seed_from_u64(SEED);
    let builder = builder.rng(rng);
    ArcSwap::new(Arc::new(builder.finish::<TagId>()))
}

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

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum ShelfInfoField {
    Name,
    Description,
    Root,
}

#[derive(Debug)]
pub struct ShelfInfo {
    pub name: StatefulField<ShelfInfoField, String>,
    pub description: StatefulField<ShelfInfoField, String>,
    pub root: StatefulField<ShelfInfoField, PathBuf>,
}

impl ShelfInfo {
    pub fn new(name: Option<String>, description: Option<String>, root_path: PathBuf) -> Self {
        let default_name = root_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("Unnamed"))
            .to_string_lossy()
            .to_string();
        let default_description = "".to_string();
        let name = name.unwrap_or(default_name);
        let description = description.unwrap_or(default_description);
        let info_state: InfoState<ShelfInfoField> = InfoState::new();
        ShelfInfo {
            name: {
                let field = StatefulField::<ShelfInfoField, String>::new(
                    ShelfInfoField::Name,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&name);
                drop(updater); // No State Update required for Info Creation
                field
            },
            description: {
                let field = StatefulField::<ShelfInfoField, String>::new(
                    ShelfInfoField::Description,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&description);
                drop(updater); // No State Update required for Info Creation
                field
            },
            root: {
                let field = StatefulField::<ShelfInfoField, PathBuf>::new(
                    ShelfInfoField::Root,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&root_path);
                drop(updater); // No State Update required for Info Creation
                field
            },
        }
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

    pub fn attach(
        &self,
        sdir_id: FileId,
        path: PathBuf,
        tag: &TagRef,
    ) -> Result<(bool, bool), UpdateErr> {
        let bind = self.dirs.pin();
        let Some(mut dir_ref) = bind.get(&sdir_id).and_then(|p| p.upgrade()) else {
            return Err(UpdateErr::PathNotFound);
        };

        let file = match dir_ref.files.pin().get(&path) {
            Some(file) => file.clone(),
            None => {
                let file_id = get_file_id(&path).map_err(|_| UpdateErr::FileNotFound)?;
                let file = File::new(
                    path,
                    papaya::HashSet::builder()
                        .shared_collector(self.root.collector.clone())
                        .build(),
                );
                let file = ImmutRef::new_ref_id(file_id, file);
                dir_ref.files.pin().insert(file.clone());
                file
            }
        };

        let file_attached = file.attach(tag);

        if file_attached {
            // traverse up to root
            while dir_ref.path != self.root.path {
                dir_ref.attach(tag, &file.clone());
                dir_ref = dir_ref
                    .parent
                    .load()
                    .as_ref()
                    .as_ref()
                    .and_then(|p| p.upgrade())
                    .ok_or(UpdateErr::PathNotFound)?; // this means that the dir has been moved
            }
        }

        let shelf_attached = if file_attached {
            dir_ref.attach(tag, &file.clone())
        } else {
            false
        };

        Ok((shelf_attached, file_attached))
    }

    pub fn detach(
        &self,
        sdir_id: FileId,
        path: PathBuf,
        tag: &TagRef,
    ) -> Result<(bool, bool), UpdateErr> {
        let bind = self.dirs.pin();
        let Some(mut dir_ref) = bind.get(&sdir_id).and_then(|p| p.upgrade()) else {
            return Err(UpdateErr::PathNotFound);
        };

        let file = match dir_ref.files.pin().get(&path) {
            Some(file) => file.clone(),
            None => {
                let file_id = get_file_id(&path).map_err(|_| UpdateErr::FileNotFound)?;
                let file = File::new(
                    path,
                    papaya::HashSet::<TagRef>::builder()
                        .shared_collector(self.root.collector.clone())
                        .build(),
                );
                let file = ImmutRef::new_ref_id(file_id, file);
                dir_ref.files.pin().insert(file.clone());
                file
            }
        };
        let file_detached = file.detach(tag);

        if file_detached {
            // traverse up to root
            while !ptr_eq(&dir_ref, self.root.inner_ptr()) {
                dir_ref.detach(tag, Some(&file.clone()));
                dir_ref = dir_ref
                    .parent
                    .load()
                    .as_ref()
                    .as_ref()
                    .and_then(|p| p.upgrade())
                    .ok_or(UpdateErr::PathNotFound)?; // this means that the dir has been moved
            }
        }

        let shelf_detached = if file_detached {
            dir_ref.detach(tag, Some(&file))
        } else {
            false
        };

        Ok((shelf_detached, file_detached))
    }

    pub fn strip(&self, sdir_id: FileId, tag: &TagRef) -> Result<(), UpdateErr> {
        let bind = self.dirs.pin();
        let Some(dir_ref) = bind.get(&sdir_id).and_then(|p| p.upgrade()) else {
            return Err(UpdateErr::PathNotFound);
        };

        pub fn recursive_remove(dir: Arc<ShelfDir>, tag: &TagRef) {
            dir.dtags.pin().remove(tag);
            dir.tags.pin().remove(tag);
            dir.dtag_dirs.pin().remove(tag);
            for file in dir.files.pin().iter() {
                file.detach(tag);
            }
            for child in dir.subdirs.pin().iter() {
                let Some(child) = child.upgrade() else {
                    break;
                };
                recursive_remove(child, tag);
            }
        }

        recursive_remove(dir_ref, tag);
        Ok(())
    }

    pub fn attach_dtag(&self, sdir_id: FileId, dtag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        let bind = self.dirs.pin();
        let Some(dir_ref) = bind.get(&sdir_id).and_then(|p| p.upgrade()) else {
            return Err(UpdateErr::PathNotFound);
        };

        let dir_attached = dir_ref.dtags.pin().insert(dtag.clone());

        let attach_dir = bind.get(&sdir_id).unwrap();
        let mut shelf_attached = true;
        while dir_ref.parent.load().is_some() {
            let Some(parent) = dir_ref.parent.load().as_ref().as_ref().unwrap().upgrade() else {
                return Err(UpdateErr::PathNotFound);
            };
            if let Some(_) = parent.dtag_dirs.pin().get(dtag) {
                parent.dtag_dirs.pin().update(dtag.clone(), |v| {
                    vec![[attach_dir.clone()].as_slice(), v].concat()
                });
                shelf_attached = false;
            } else {
                parent
                    .dtag_dirs
                    .pin()
                    .insert(dtag.clone(), vec![attach_dir.clone()]);
                shelf_attached = true;
            }
        }

        fn recursive_attach(dir: &ShelfDirRef, dtag: &TagRef) {
            if let Some(dir) = dir.upgrade() {
                for subdir in dir.subdirs.pin().iter() {
                    recursive_attach(&subdir, dtag);
                }

                dir.dtags.pin().insert(dtag.clone());
            }
        }

        recursive_attach(attach_dir, dtag);

        Ok((shelf_attached, dir_attached))
    }
    pub fn detach_dtag(&self, sdir_id: FileId, dtag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        let bind = self.dirs.pin();
        let Some(dir_ref) = bind.get(&sdir_id).and_then(|p| p.upgrade()) else {
            return Err(UpdateErr::PathNotFound);
        };

        let dir_detached = dir_ref.dtags.pin().remove(dtag);

        let detach_dir = bind.get(&sdir_id).unwrap();
        let mut shelf_detached = true;
        while dir_ref.parent.load().is_some() {
            let Some(parent) = dir_ref.parent.load().as_ref().as_ref().unwrap().upgrade() else {
                return Err(UpdateErr::PathNotFound);
            };
            parent.dtag_dirs.pin().update(dtag.clone(), |v| {
                v.into_iter().cloned().filter(|n| n != detach_dir).collect()
            });
            if let Some(vec_dir) = parent.dtag_dirs.pin().get(dtag) {
                if vec_dir.is_empty() {
                    parent.dtag_dirs.pin().remove(dtag);
                    shelf_detached = true;
                } else {
                    shelf_detached = false;
                }
            }
        }

        fn recursive_detach(dir: &ShelfDirRef, dtag: &TagRef) {
            if let Some(dir) = dir.upgrade() {
                for subdir in dir.subdirs.pin().iter() {
                    recursive_detach(&subdir, dtag);
                }

                dir.dtags.pin().remove(dtag);
            }
        }

        recursive_detach(detach_dir, dtag);

        Ok((shelf_detached, dir_detached))
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

#[cfg(test)]
mod tests {
    use crate::{
        sharedref::{ImmutRef, Inner, SharedRef},
        shelf::file::{File, FileMetadata, FileRef},
        tag::Tag,
    };
    use jwalk::WalkDir;
    use papaya::HashSet;
    use rayon::prelude::*;
    use std::fs::{self, File as FileIO};
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    fn list_files(root: PathBuf) -> Vec<FileRef> {
        WalkDir::new(root)
            .into_iter()
            .filter_map(|entry_res| {
                let entry = entry_res.unwrap();
                if entry.file_type().is_file() {
                    Some(SharedRef::new_ref(File::new(
                        entry.path().clone(),
                        HashSet::new(),
                        HashSet::new(),
                        FileMetadata::new(&entry.path()),
                    )))
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn attach() {
        let dir_path = PathBuf::from("target/test");
        fs::create_dir_all(&dir_path).unwrap();
        let file_path0 = dir_path.join("file.jpg");
        let file_path1 = dir_path.join("file.txt");
        let _ = FileIO::create(&file_path0).unwrap();
        let _ = FileIO::create(&file_path1).unwrap();

        let shelf = ShelfData::new(dir_path.clone()).unwrap();

        let tag = Tag {
            priority: 0,
            name: "tag0".to_string(),
            parent: None,
        };
        let tag_ref: Arc<Inner<Tag>> = ImmutRef::new_ref(tag.clone());
        let (shelf_attached, file_attached) = shelf.attach(file_path0.clone(), &tag_ref).unwrap();

        // newly attached to (shelf, file)
        assert_eq!((shelf_attached, file_attached), (true, true));
        assert!(shelf.root.tags.pin().contains_key(&tag_ref));

        let (shelf_attached, file_attached) = shelf.attach(file_path0.clone(), &tag_ref).unwrap();

        // not newly attached to (shelf, file)
        assert_eq!((shelf_attached, file_attached), (false, false));

        let (shelf_attached, file_attached) = shelf.attach(file_path1.clone(), &tag_ref).unwrap();

        // newly attached to file only
        assert_eq!((shelf_attached, file_attached), (false, true));

        let tag_ref: Arc<Inner<Tag>> = ImmutRef::new_ref(tag);

        let res = shelf.attach(file_path0.clone(), &tag_ref).unwrap();

        // another new tag
        assert_eq!(res, (true, true));
        assert!(shelf.root.tags.pin().contains_key(&tag_ref));

        fs::remove_file(&file_path0).unwrap();
        fs::remove_file(&file_path1).unwrap();
        fs::remove_dir(&dir_path).unwrap();
    }
}
