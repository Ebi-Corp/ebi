use crate::prelude::*;
use crate::sharedref::WeakRef;
use crate::shelf::file::{File, FileRef};
use crate::tag::TagRef;
use file_id::{FileId, get_file_id};
use jwalk::WalkDir;
use seize::Collector;
use std::hash::RandomState;
use std::io;
use std::path::PathBuf;

pub type NodeRef = WeakRef<Node, FileId>;
pub type HashSet<T> = papaya::HashSet<T, RandomState, Arc<Collector>>;
pub type HashMap<K, V> = papaya::HashMap<K, V, RandomState, Arc<Collector>>;

pub struct Node {
    pub path: PathBuf,
    pub files: HashSet<FileRef>,
    pub collector: Arc<Collector>,
    pub tags: HashMap<TagRef, HashSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // dtags applied from above, to be applied down
    pub dtag_nodes: HashMap<TagRef, Vec<NodeRef>>, // list of dtagged directories starting at any point below
    pub parent: Option<NodeRef>,
    pub subdirs: HashSet<NodeRef>,
}

impl Node {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let collector = Arc::new(Collector::new());
        let files: HashSet<FileRef> = WalkDir::new(path.clone())
            .follow_links(false)
            .skip_hidden(false)
            .sort(false)
            .into_iter()
            .flat_map(|entry_res| {
                let entry = entry_res.unwrap();
                if entry.file_type().is_file() {
                    if let Ok(id) = get_file_id(entry.path()) {
                        Some(FileRef::new_ref_id(
                            id,
                            File::new(
                                entry.path().clone(),
                                papaya::HashSet::builder()
                                    .shared_collector(collector.clone())
                                    .build(),
                            ),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        Ok(Node {
            files,
            path,
            collector,
            tags: HashMap::default(),
            dtags: HashSet::default(),
            dtag_nodes: HashMap::default(),
            parent: None,
            subdirs: HashSet::default(),
        })
    }

    /*
    pub fn full_init(path: PathBuf) -> Result<Self, io::Error> {
        let mut dirs = Vec::<PathBuf>::new();
        let collector = Arc::new(Collector::new());
        let files = walk_dir_init(&path, &mut dirs, &collector);
        let directories = dirs
            .into_iter()
            .map(|dir| {
                (
                    dir.strip_prefix(&path).unwrap().to_path_buf(),
                    Arc::new(Node::full_init(dir).unwrap()),
                )
            })
            .collect::<HashMap<PathBuf, Arc<Node>>>();
        Ok(Node {
            path,
            files,
            collector,
            tags: HashMap::new(),
            dtags: HashSet::new(),
            dtag_files: HashMap::new(),
            parent: None, // TODO: Initialize properly
            subdirs: HashSet::new(),
        })
    }
    */

    pub fn attach_dtag(&self, dtag: &TagRef) -> bool {
        self.dtags.pin().insert(dtag.clone())
    }

    pub fn detach_dtag(&self, dtag: &TagRef) -> bool {
        self.dtags.pin().remove(dtag)
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

/*
fn walk_dir_init(
    root: &PathBuf,
    dirs: &mut Vec<PathBuf>,
    collector: &Arc<Collector>,
) -> HashSet<FileRef> {
    let mut visited_root = false;
    WalkDir::new(root)
        .follow_links(false)
        .skip_hidden(false)
        .sort(false)
        .into_iter()
        .flat_map(|entry_res| {
            let entry = entry_res.unwrap();
            if entry.file_type().is_file() {
                if let Ok(id) = get_file_id(entry.path()) {
                    Some(FileRef::new_ref_id(
                        id,
                        File::new(
                            entry.path().clone(),
                            papaya::HashSet::builder().collector(collector.clone()).build(),
                            papaya::HashSet::builder().collector(collector.clone()).build(),
                        ),
                    ))
                } else {
                    None
                }
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
*/
