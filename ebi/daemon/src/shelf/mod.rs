pub mod file;
pub mod node;
use crate::prelude::*;
use crate::shelf::node::Node;
use crate::stateful::{InfoState, StatefulField};
use crate::tag::{Tag, TagId};
use crate::workspace::ChangeSummary;
use arc_swap::ArcSwap;
use chrono::Duration;
use iroh::NodeId;
use papaya::HashSet;
use rand_chacha::{ChaCha12Rng, rand_core::SeedableRng};
use rayon::prelude::*;
use scalable_cuckoo_filter::{ScalableCuckooFilter, ScalableCuckooFilterBuilder};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::result::Result;
use tokio::sync::RwLock;

use file::FileRef;

pub type ShelfId = Uuid;
pub type TagRef = ImmutRef<Tag>;

const SEED: u64 = 0; // [TODO] Move seed to proper initialization

pub type ShelfDataRef = ImmutRef<ShelfData>;
pub type TagFilter =
    ScalableCuckooFilter<TagId, scalable_cuckoo_filter::DefaultHasher, ChaCha12Rng>;

#[derive(Clone, Debug)]
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

#[derive(Debug)]
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
        remote: bool,
        path: PathBuf,
        name: String,
        shelf_owner: ShelfOwner,
        config: Option<ShelfConfig>,
        description: String,
    ) -> Result<Shelf, io::Error> {
        let shelf_type = if remote {
            ShelfType::Remote
        } else {
            let shelf_data = ShelfData::new(path.clone())?;
            ShelfType::Local(ImmutRef::new_ref(shelf_data))
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

#[derive(Debug)]
pub struct ShelfData {
    pub root: Arc<Node>,
    pub root_path: PathBuf,
}
impl Clone for ShelfData {
    fn clone(&self) -> Self {
        ShelfData {
            root: self.root.clone(),
            root_path: self.root_path.clone(),
        }
    }
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
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        Ok(ShelfData {
            root: Arc::new(Node::new(path.clone())?),
            root_path: path.clone(),
        })
    }

    pub async fn files(&self) -> Vec<FileRef> {
        self.root.files.pin().values().cloned().collect()
    }

    pub fn contains(&self, tag: TagRef) -> bool {
        self.root.tags.pin().contains_key(&tag) || self.root.dtag_files.pin().contains_key(&tag)
    }

    pub async fn refresh(&self) -> Result<ChangeSummary, io::Error> {
        todo!();
    }

    pub fn attach(&self, path: PathBuf, tag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        // [/] newly attached to (shelf, file)
        let stripped_path = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?
            .parent();

        let mut node_ref: Vec<Arc<Node>> = Vec::new();
        // load the root Arc
        let mut curr_node = self.root.clone();

        if let Some(path) = stripped_path {
            for dir in path.components().as_path() {
                let child = (curr_node
                    .directories
                    .pin()
                    .get(&PathBuf::from(&dir))
                    .ok_or(UpdateErr::PathNotFound)?)
                .clone();

                node_ref.push(child.clone());
                curr_node = child;
            }
        }

        let file_pin = curr_node.files.pin();

        let file = file_pin.get(&path).ok_or(UpdateErr::FileNotFound)?;
        let file_attached = file.load().attach(tag);

        for node in node_ref.into_iter().rev() {
            if file_attached {
                node.attach(tag, file);
            }
        }

        let shelf_attached = if file_attached {
            curr_node.attach(tag, file)
        } else {
            false
        };

        Ok((shelf_attached, file_attached))
    }

    pub fn detach(&self, path: PathBuf, tag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        // [/] newly attached to (shelf, file)
        let stripped_path = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?
            .parent();

        let mut node_ref: Vec<Arc<Node>> = Vec::new();
        // load the root Arc
        let mut curr_node = self.root.clone();

        if let Some(path) = stripped_path {
            for dir in path.components().as_path() {
                let child = (curr_node
                    .directories
                    .pin()
                    .get(&PathBuf::from(&dir))
                    .ok_or(UpdateErr::PathNotFound)?)
                .clone();

                node_ref.push(child.clone());
                curr_node = child;
            }
        }

        let file_pin = curr_node.files.pin();

        let file = file_pin.get(&path).ok_or(UpdateErr::FileNotFound)?;
        let file_detached = file.load().detach(tag);

        for node in node_ref.into_iter().rev() {
            if file_detached {
                node.detach(tag, Some(file));
            }
        }

        let shelf_attached = if file_detached {
            curr_node.detach(tag, Some(file))
        } else {
            false
        };

        Ok((shelf_attached, file_detached))
    }

    pub fn strip(&self, path: PathBuf, tag: &TagRef) -> Result<(), UpdateErr> {
        let mut curr_node = self.root.clone();

        if !path.is_dir() {
            return Err(UpdateErr::PathNotDir);
        }

        for dir in path.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .pin()
                .get(&dir)
                .ok_or(UpdateErr::PathNotFound)?
                .clone();
            curr_node = child;
        }

        pub fn recursive_remove(node: Arc<Node>, tag: &TagRef) {
            node.dtags.pin().remove(tag);
            node.tags.pin().remove(tag);
            node.dtag_files.pin().remove(tag);
            for file in node.files.pin().values() {
                file.load().detach(tag);
            }
            node.directories.pin().values().for_each(|child| {
                recursive_remove(child.clone(), tag);
            });
        }

        recursive_remove(curr_node, tag);
        Ok(())
    }

    pub fn attach_dtag(&self, path: PathBuf, dtag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        // [/] newly attached to (shelf, dir)
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = self.root.clone();
        let mut node_ref: Vec<Arc<Node>> = Vec::new();
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .pin()
                .get(&dir)
                .ok_or(UpdateErr::PathNotFound)?
                .clone();
            if child.dtags.pin().contains(dtag) {
                dtagged_parent = true;
            }

            node_ref.push(child.clone());
            curr_node = child;
        }

        if dtagged_parent {
            return Ok((false, curr_node.attach_dtag(dtag)));
        }

        let shelf_attached = !curr_node.dtag_files.pin().contains_key(dtag);

        let dir_attached = curr_node.attach_dtag(dtag);

        fn recursive_attach(node: Arc<Node>, dtag: &TagRef) -> std::collections::HashSet<FileRef> {
            let mut files: std::collections::HashSet<FileRef> =
                node.files.pin().values().cloned().collect();
            for (_, subnode) in node.directories.pin().iter() {
                files.extend(recursive_attach(subnode.clone(), dtag));
            }

            node.dtag_files
                .pin()
                .get_or_insert_with(dtag.clone(), HashSet::new)
                .extend(files.clone());
            files
        }

        let files = recursive_attach(curr_node, dtag);

        files.par_iter().for_each(|f| {
            f.load().attach_dtag(dtag);
        });

        Ok((shelf_attached, dir_attached))
    }
    pub fn detach_dtag(&self, path: PathBuf, dtag: &TagRef) -> Result<(bool, bool), UpdateErr> {
        // eliminated from (shelf, file)
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = self.root.clone();
        let mut node_ref: Vec<Arc<Node>> = Vec::new();

        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .pin()
                .get(&dir)
                .ok_or(UpdateErr::PathNotFound)?
                .clone();
            if child.dtags.pin().contains(dtag) {
                dtagged_parent = true;
            }
            node_ref.push(child.clone());
            curr_node = child;
        }

        if dtagged_parent {
            return Ok((false, curr_node.detach_dtag(dtag)));
        }

        let dir_detached = curr_node.detach_dtag(dtag);

        fn recursive_detach(node: Arc<Node>, dtag: &TagRef) -> std::collections::HashSet<FileRef> {
            // Stop detaching the dtag when encountering a child node already dtagged with it
            if node.dtags.pin().contains(dtag) {
                return std::collections::HashSet::new();
            }
            let mut files: std::collections::HashSet<FileRef> =
                node.files.pin().values().cloned().collect();

            for (_, subnode) in node.directories.pin().iter() {
                files.extend(recursive_detach(subnode.clone(), dtag))
            }

            if let Some(set) = node.dtag_files.pin().get(dtag) {
                files.iter().for_each(|f| {
                    set.pin().remove(f);
                });
                if set.pin().is_empty() {
                    node.dtag_files.pin().remove(dtag);
                }
            }
            files
        }

        let files = recursive_detach(curr_node, dtag);

        files.par_iter().for_each(|f| {
            f.load().detach_dtag(dtag);
        });

        let shelf_detached = !self.root.clone().dtag_files.pin().contains_key(dtag);

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
