use crate::shelf::file::{File, FileMetadata, FileRef};
use crate::tag::TagRef;
use core::hash;
use jwalk::WalkDir;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Default)]
pub struct Node {
    pub files: HashMap<PathBuf, FileRef>,
    pub tags: HashMap<TagRef, HashSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // directory level tags, to be applied down
    pub dtag_files: HashMap<TagRef, HashSet<FileRef>>, // files tagged with directory level tags
    pub directories: HashMap<PathBuf, Node>, // untagged: set of untagged files, support structure
}

impl Node {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let mut dirs = Vec::<PathBuf>::new();
        let files = walk_dir_init(&path, &mut dirs);
        let directories = dirs
            .into_iter()
            .map(|dir| {
                (
                    dir.strip_prefix(&path).unwrap().to_path_buf(),
                    Node::new(dir).unwrap(),
                )
            })
            .collect::<HashMap<PathBuf, Node>>();
        Ok(Node {
            files,
            tags: HashMap::new(),
            dtags: HashSet::new(),
            dtag_files: HashMap::new(),
            directories,
        })
    }

    pub fn attach_dtag(&mut self, dtag: TagRef) -> bool {
        self.dtags.insert(dtag)
    }

    pub fn detach_dtag(&mut self, dtag: TagRef) -> bool {
        self.dtags.remove(&dtag)
    }

    pub fn attach(&mut self, tag: TagRef, file: FileRef) -> bool {
        let existed = matches!(self.tags.entry(tag.clone()), Entry::Occupied(_));
        let set = self.tags.entry(tag).or_default();
        set.insert(file.clone());
        !existed
    }

    pub fn detach(&mut self, tag: TagRef, file: Option<FileRef>) -> bool {
        match file {
            Some(file) => {
                if let Some(set) = self.tags.get_mut(&tag) {
                    let res = set.remove(&file);
                    if set.is_empty() {
                        self.tags.remove(&tag);
                    }
                    res
                } else {
                    false
                }
            }
            None => self.tags.remove(&tag).is_some(),
        }
    }
}

fn walk_dir_init(root: &PathBuf, dirs: &mut Vec<PathBuf>) -> HashMap<PathBuf, FileRef> {
    let mut visited_root = false;
    // initialize unsorted walkdir that doesn't follow symlinks and includes hidden files
    WalkDir::new(root)
        .follow_links(false)
        .skip_hidden(false)
        .sort(false)
        .into_iter()
        .flat_map(|entry_res| {
            let entry = entry_res.unwrap();
            if entry.file_type().is_file() {
                Some((
                    entry.path(),
                    FileRef {
                        file_ref: Arc::new(RwLock::new(File::new(
                            entry.path().clone(),
                            HashSet::new(),
                            HashSet::new(),
                            FileMetadata::new(&entry.path()),
                        ))),
                    },
                ))
            } else if entry.file_type().is_dir() {
                if !visited_root {
                    visited_root = true
                } else {
                    dirs.push(entry.path());
                }
                None
            } else {
                None
            }
        })
        .collect()
}
