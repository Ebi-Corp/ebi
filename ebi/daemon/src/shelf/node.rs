use crate::shelf::file::{File, FileMetadata, FileRef};
use crate::tag::TagRef;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Default)]
pub struct Node {
    pub files: HashMap<PathBuf, FileRef>,
    pub tags: HashMap<TagRef, BTreeSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // directory level tags, to be applied down
    pub dtag_files: HashMap<TagRef, BTreeSet<FileRef>>, // files tagged with directory level tags
    pub directories: HashMap<PathBuf, Node>, // untagged: set of untagged files, support structure
}

impl Node {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let entries = std::fs::read_dir(&path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        let (dir_paths, file_paths): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|path| path.is_dir());
        let files = file_paths
            .into_iter()
            .map(|file_path| {
                (
                    file_path.clone(),
                    FileRef {
                        file_ref: Arc::new(RwLock::new(File::new(
                            file_path.clone(),
                            BTreeSet::new(),
                            BTreeSet::new(),
                            FileMetadata::new(&file_path),
                        ))),
                    },
                )
            })
            .collect::<HashMap<PathBuf, FileRef>>();
        let directories = dir_paths
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
        let set = self.tags.entry(tag).or_default();
        set.insert(file.clone())
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
