use crate::sharedref::{Ref, SharedRef, ImmutRef};
use crate::shelf::file::{File, FileMetadata};
use crate::tag::Tag;
use jwalk::WalkDir;
use papaya::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

type FileRef = SharedRef<File>;
type TagRef = ImmutRef<Tag>;

#[derive(Debug, Default)]
pub struct Node {
    pub files: HashMap<PathBuf, FileRef>,
    pub tags: HashMap<TagRef, HashSet<FileRef>>,
    pub dtags: HashSet<TagRef>, // directory level tags, to be applied down
    pub dtag_files: HashMap<TagRef, HashSet<FileRef>>, // files tagged with directory level tags
    pub directories: HashMap<PathBuf, Arc<Node>>,
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
                    Arc::new(Node::new(dir).unwrap()),
                )
            })
            .collect::<HashMap<PathBuf, Arc<Node>>>();
        Ok(Node {
            files,
            tags: HashMap::new(),
            dtags: HashSet::new(),
            dtag_files: HashMap::new(),
            directories,
        })
    }

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
            let new_set = HashSet::new();
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

fn walk_dir_init(root: &PathBuf, dirs: &mut Vec<PathBuf>) -> HashMap<PathBuf, FileRef> {
    let mut visited_root = false;
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
                    SharedRef::new_ref_id(
                        Uuid::new_v5(
                            &Uuid::NAMESPACE_URL,
                            entry.path().as_os_str().as_encoded_bytes(),
                        ),
                        File::new(
                            entry.path().clone(),
                            HashSet::new(),
                            HashSet::new(),
                            FileMetadata::new(&entry.path()),
                        )
                    ),
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
