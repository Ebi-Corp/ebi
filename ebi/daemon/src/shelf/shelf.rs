use crate::shelf::node::Node;
use crate::tag::{TagId, TagRef};
use crate::workspace::ChangeSummary;
use chrono::Duration;
use iroh::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::file::FileRef;

pub type ShelfId = Uuid;

pub type ShelfRef = Arc<RwLock<Shelf>>;
pub type ShelfDataRef = Arc<RwLock<ShelfData>>;

#[derive(Clone, Debug)]
pub enum ShelfType {
    Local(ShelfDataRef),
    Remote,
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum ShelfOwner {
    Node(NodeId),
    Sync(SyncId),
}

//[#] Sync

pub type SyncId = Uuid;

#[derive(Debug, Clone)]
pub struct SyncConfig {
    //[!] Placeholder for sync configuration
    pub interval: Option<Duration>, // Auto-Sync Interval
    pub auto_sync: bool,            // Auto-Sync on changes
}

//[#] Sync

#[derive(Debug, Clone, Default)]
pub struct ShelfConfig {
    //[TODO] Define a configuration for the shelf
    pub sync_config: Option<SyncConfig>,
}

#[derive(Clone, Debug)]
pub struct Shelf {
    pub shelf_type: ShelfType,
    pub shelf_owner: ShelfOwner,
    pub config: ShelfConfig,
    pub info: ShelfInfo,
}

impl Shelf {

    pub fn get_tags(&self) -> HashSet<TagId> {
        todo!(); //[TODO] Cuckoo Table 
    }
    
    pub async fn edit_info(&mut self, new_name: Option<String>, new_description: Option<String>) {
        if let Some(name) = new_name {
            self.info.name = name;
        }
        if let Some(description) = new_description {
            self.info.description = description;
        }
    }

    pub fn new(
        remote: bool,
        path: PathBuf,
        name: String,
        shelf_owner: ShelfOwner,
        config: Option<ShelfConfig>,
        description: String,
    ) -> Result<Shelf, io::Error> {
        let id = Uuid::now_v7();
        let shelf_type = if remote {
            ShelfType::Remote
        } else {
            let shelf_data = ShelfData::new(path.clone())?;
            ShelfType::Local(Arc::new(RwLock::new(shelf_data)))
        };
        let shelf = Shelf {
            shelf_type,
            shelf_owner,
            config: config.unwrap_or(ShelfConfig::default()),
            info: ShelfInfo {
                id,
                name,
                description,
                root_path: path,
            },
        };
        Ok(shelf)
    }
}

#[derive(Debug)]
pub struct ShelfData {
    root: Node,
    root_path: PathBuf,
}

impl PartialEq for ShelfData {
    fn eq(&self, other: &Self) -> bool {
        self.root_path == other.root_path
    }
}

#[derive(Clone, Debug)]
pub struct ShelfInfo {
    pub id: ShelfId,
    pub name: String,
    pub description: String,
    pub root_path: PathBuf, //[/] This is to minimise lock acquisition for ShelfData
                            //summary: ShelfSummary
}

impl ShelfInfo {
    pub fn new(
        id: ShelfId,
        name: Option<String>,
        description: Option<String>,
        root_path: String,
    ) -> Self {
        let root_path = PathBuf::from(root_path);
        let default_name = root_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("Unnamed"))
            .to_string_lossy()
            .to_string();
        let default_description = "".to_string();
        let name = name.unwrap_or_else(|| default_name);
        let description = description.unwrap_or_else(|| default_description);
        ShelfInfo {
            id,
            name,
            description,
            root_path,
        }
    }
}

impl ShelfData {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        Ok(ShelfData {
            root: Node::new(path.clone())?,
            root_path: path.clone(),
        })
    }

    pub async fn retrieve(&self, tag: TagRef) -> BTreeSet<FileRef> {
        let mut res = self
            .root
            .tags
            .get(&tag)
            .cloned()
            .unwrap_or_else(|| BTreeSet::<FileRef>::new());
        let mut dres = self
            .root
            .dtag_files
            .get(&tag)
            .cloned()
            .unwrap_or_else(|| BTreeSet::<FileRef>::new());
        res.append(&mut dres);
        res
    }

    pub fn contains(&self, tag: TagRef) -> bool {
        self.root.tags.contains_key(&tag) || self.root.dtag_files.contains_key(&tag)
    }

    pub async fn refresh(&self) -> Result<ChangeSummary, io::Error> {
        todo!();
        //[TODO] Run automatic tagging on all new or modified files in the shelf
    }

    pub fn attach(&mut self, path: PathBuf, tag: TagRef) -> Result<bool, UpdateErr> {
        let stripped_path = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?
            .parent();

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if stripped_path is none, file must be self.root
        if let Some(path) = stripped_path {
            for dir in path.components() {
                let dir: PathBuf = dir.as_os_str().into();

                let child = curr_node
                    .directories
                    .remove(&dir)
                    .ok_or_else(|| UpdateErr::PathNotFound)?;

                // Store the current node (ownership moved)
                node_v.push((dir.to_path_buf(), curr_node));

                // Move to the child node
                curr_node = child;
            }
        }

        let file = curr_node
            .files
            .get(&path)
            .ok_or_else(|| UpdateErr::FileNotFound)?;
        let file = file.clone();
        let res = file.file_ref.write().unwrap().attach(tag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            if res {
                node.attach(tag.clone(), file.clone());
            }
            let child = std::mem::replace(&mut curr_node, node);
            curr_node.directories.insert(pbuf, child);
        }

        self.root = curr_node;
        Ok(res)
    }

    pub fn detach(&mut self, path: PathBuf, tag: TagRef) -> Result<bool, UpdateErr> {
        let stripped_path = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?
            .parent();

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if stripped_path is none, file must be self.root
        if let Some(path) = stripped_path {
            for dir in path.components() {
                let dir: PathBuf = dir.as_os_str().into();

                let child = curr_node
                    .directories
                    .remove(&dir)
                    .ok_or_else(|| UpdateErr::PathNotFound)?;
                // Store the current node (ownership moved)
                node_v.push((dir.to_path_buf(), curr_node));

                // Move to the child node
                curr_node = child;
            }
        }

        let file = curr_node
            .files
            .get(&path)
            .ok_or_else(|| UpdateErr::FileNotFound)?;
        let file = file.clone();
        let res = file.file_ref.write().unwrap().detach(tag.clone());
        for (pbuf, mut node) in node_v.into_iter().rev() {
            if res {
                node.detach(tag.clone(), Some(file.clone()));
            }
            let child = std::mem::replace(&mut curr_node, node);
            curr_node.directories.insert(pbuf, child);
        }

        self.root = curr_node;
        Ok(res)
    }

    pub fn attach_dtag(&mut self, path: PathBuf, dtag: TagRef) -> Result<bool, UpdateErr> {
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = &mut self.root;
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            if child.dtags.contains(&dtag) {
                dtagged_parent = true;
            }
            curr_node = child;
        }
        if dtagged_parent {
            return Ok(curr_node.attach_dtag(dtag.clone()));
        }

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if dpath is none, dir must be self.root
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .remove(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            // Store the current node (ownership moved)
            node_v.push((dir.to_path_buf(), curr_node));
            // Move to the child node
            curr_node = child;
        }

        let res = curr_node.attach_dtag(dtag.clone());

        fn recursive_attach(node: &mut Node, dtag: TagRef) -> BTreeSet<FileRef> {
            let mut files = node.files.values().cloned().collect::<BTreeSet<FileRef>>();
            let mut subdir_files = BTreeSet::new();
            for (_, subnode) in node.directories.iter_mut() {
                let mut sub_files = recursive_attach(subnode, dtag.clone());
                subdir_files.append(&mut sub_files);
            }
            files.append(&mut subdir_files);

            fn add_dtag_files(node: &mut Node, dtag: TagRef, files: BTreeSet<FileRef>) {
                let set = node
                    .dtag_files
                    .entry(dtag)
                    .or_insert_with(|| BTreeSet::new());
                files.iter().for_each(|f| {
                    set.insert(f.clone());
                });
            }

            add_dtag_files(node, dtag, files.clone());
            return files;
        }

        let files = recursive_attach(&mut curr_node, dtag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            let set = node
                .dtag_files
                .entry(dtag.clone())
                .or_insert_with(|| BTreeSet::new());
            set.append(&mut files.clone());
            let child = std::mem::replace(&mut curr_node, node);
            curr_node.directories.insert(pbuf, child);
        }

        files.iter().for_each(|f| {
            f.file_ref.write().unwrap().attach_dtag(dtag.clone());
        });

        Ok(res)
    }

    pub fn detach_dtag(&mut self, path: PathBuf, dtag: TagRef) -> Result<bool, UpdateErr> {
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = &mut self.root;
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            if child.dtags.contains(&dtag) {
                dtagged_parent = true;
            }
            curr_node = child;
        }
        if dtagged_parent {
            return Ok(curr_node.detach_dtag(dtag.clone()));
        }

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if dpath is none, dir must be self.root
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .remove(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            // Store the current node (ownership moved)
            node_v.push((dir.to_path_buf(), curr_node));
            // Move to the child node
            curr_node = child;
        }

        let res = curr_node.detach_dtag(dtag.clone());

        fn recursive_detach(node: &mut Node, dtag: TagRef) -> BTreeSet<FileRef> {
            // Stop detaching the dtag when encountering a child node already dtagged with it
            if node.dtags.contains(&dtag) {
                return BTreeSet::new();
            }

            let mut files = node.files.values().cloned().collect::<BTreeSet<FileRef>>();
            let mut subdir_files = BTreeSet::new();
            for (_, subnode) in node.directories.iter_mut() {
                let mut sub_files = recursive_detach(subnode, dtag.clone());
                subdir_files.append(&mut sub_files);
            }
            files.append(&mut subdir_files);

            fn remove_dtag_files(node: &mut Node, dtag: TagRef, files: BTreeSet<FileRef>) {
                let set = node.dtag_files.get_mut(&dtag);
                match set {
                    Some(set) => {
                        files.iter().for_each(|f| {
                            set.remove(f);
                        });
                        if set.is_empty() {
                            node.dtag_files.remove(&dtag);
                        }
                    }
                    None => (), // Critical Internal Error
                }
            }

            remove_dtag_files(node, dtag, files.clone());
            return files;
        }

        let files = recursive_detach(&mut curr_node, dtag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            let set = node.dtag_files.get_mut(&dtag.clone());
            match set {
                Some(set) => {
                    files.iter().for_each(|f| {
                        set.remove(f);
                    });
                    let child = std::mem::replace(&mut curr_node, node);
                    curr_node.directories.insert(pbuf, child);
                }
                None => (), // Internal Error
            }
        }

        files.iter().for_each(|f| {
            f.file_ref.write().unwrap().detach_dtag(dtag.clone());
        });

        Ok(res)
    }

    pub fn strip(&mut self, path: PathBuf, tag: TagRef) -> Result<(), UpdateErr> {
        let mut curr_node = &mut self.root;

        if !path.is_dir() {
            return Err(UpdateErr::PathNotDir);
        }

        for dir in path.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            curr_node = child;
        }

        fn recursive_remove(node: &mut Node, tag: TagRef) {
            node.dtags.remove(&tag);
            node.tags.remove(&tag);
            node.dtag_files.remove(&tag);
            node.files.values_mut().for_each(|file| {
                file.file_ref.write().unwrap().detach(tag.clone());
            });
            node.directories.values_mut().for_each(|child| {
                recursive_remove(child, tag.clone());
            });
        }

        recursive_remove(curr_node, tag);
        Ok(())
    }
}

#[derive(Debug)]
pub enum UpdateErr {
    PathNotFound,
    FileNotFound,
    PathNotDir,
}
