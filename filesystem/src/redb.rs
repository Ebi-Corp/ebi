use crate::{dir::ShelfDir, file::File, shelf::ShelfData};
use ebi_types::redb::*;
use ebi_types::{FileId, Uuid, tag::Tag};
use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

pub const T_SHELF_DATA: TableDefinition<FileId, Bincode<ShelfData>> =
    TableDefinition::new("shelf_data");
pub const T_SHELF_DIR: TableDefinition<FileId, Bincode<ShelfDir>> =
    TableDefinition::new("shelf_dir");
pub const T_FILE: TableDefinition<FileId, Bincode<File>> = TableDefinition::new("file");
pub const T_TAG: TableDefinition<Uuid, Bincode<Tag>> = TableDefinition::new("tag");

#[derive(Debug, Deserialize, Serialize)]
pub struct ShelfDataStorable {
    pub root: FileId,
    pub dirs: Vec<FileId>,
    pub root_path: PathBuf,
}

impl Storable for ShelfData {
    type Storable = ShelfDataStorable;

    fn to_storable(&self) -> Bincode<ShelfData> {
        Bincode(ShelfDataStorable {
            root: self.root.id,
            dirs: self.dirs.pin().iter().map(|f| f.id).collect(),
            root_path: self.root_path.clone(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShelfDirStorable {
    pub path: PathBuf,
    pub files: Vec<FileId>,
    pub tags: HashMap<Uuid, Vec<FileId>>,      // Tag -> Files
    pub dtags: Vec<Uuid>,                      // dtags applied from above, to be applied down
    pub dtag_dirs: HashMap<Uuid, Vec<FileId>>, // Dtag -> Dirs
    pub parent: Option<FileId>,                // parent dir id
    pub subdirs: Vec<FileId>,                  // dirs
}

impl Storable for ShelfDir {
    type Storable = ShelfDirStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(ShelfDirStorable {
            path: self.path.clone(),
            files: self.files.pin().iter().map(|f| f.id).collect(),
            tags: self
                .tags
                .pin()
                .iter()
                .map(|(tag, set)| (tag.id, set.pin().iter().map(|f| f.id).collect()))
                .collect(),
            dtags: self.dtags.pin().iter().map(|t| t.id).collect(),
            dtag_dirs: self
                .dtag_dirs
                .pin()
                .iter()
                .map(|(tag, set)| (tag.id, set.iter().map(|f| f.id).collect()))
                .collect(),
            parent: self.parent.load().as_ref().clone().and_then(|s| Some(s.id)),
            subdirs: self.subdirs.pin().iter().map(|d| d.id).collect(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileStorable {
    pub path: PathBuf,
    pub tags: Vec<Uuid>,
}

impl Storable for File {
    type Storable = FileStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(FileStorable {
            path: self.path.clone(),
            tags: self.tags.pin().iter().map(|t| t.id).collect(),
        })
    }
}

#[cfg(test)] //[!] Check 
mod tests {
    use std::sync::Arc;

    use crate::service::FileSystem;
    use crate::service::ShelfDirKey;

    use super::*;
    use ::redb::{Database, ReadableDatabase};
    use ebi_types::*;
    use papaya::HashSet;

    #[tokio::test]
    async fn test_db() {
        let path = PathBuf::from(".");
        let db = Database::create("test_db").unwrap();

        let mut fs = FileSystem {
            local_shelves: Arc::new(HashSet::new()),
            shelf_dirs: Arc::new(HashSet::new()),
            db: Arc::new(db),
        };
        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(PathBuf::from("../../")))
            .await
            .unwrap();
        let s_2 = fs
            .get_or_init_shelf(ShelfDirKey::Path(PathBuf::from("../../target/")))
            .await
            .unwrap();
        let _ = fs
            .get_or_init_dir(ShelfDirKey::Id(s.id), PathBuf::from("daemon"))
            .await;
        let _ = fs
            .get_or_init_dir(ShelfDirKey::Id(s.id), PathBuf::from("database"))
            .await;
        let _ = fs
            .get_or_init_dir(ShelfDirKey::Id(s.id), PathBuf::from("filesystem"))
            .await;
        let _ = fs
            .get_or_init_dir(ShelfDirKey::Id(s.id), PathBuf::from("filesystem/src"))
            .await;

        //let fs = FileSystem::full_load("test_db").await.unwrap();
    }
}
