use crate::{dir::ShelfDir, file::File, shelf::ShelfData};
use ebi_types::{FileId, Uuid, tag::Tag};
use ebi_types::{SharedRef, WithPath, redb::*};
use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

pub const T_SHELF_DATA: TableDefinition<FileId, Bincode<ShelfData>> =
    TableDefinition::new("shelf_data");
pub const T_SHELF_DIR: TableDefinition<FileId, Bincode<ShelfDir>> =
    TableDefinition::new("shelf_dir");
pub const T_FILE: TableDefinition<FileId, Bincode<File>> = TableDefinition::new("file");
pub const T_TAG: TableDefinition<Uuid, Bincode<SharedRef<Tag>>> = TableDefinition::new("tag");

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
            path: self.path(),
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
            parent: self.parent.load().as_ref().clone().map(|s| s.id),
            subdirs: self.subdirs.pin().iter().map(|d| d.id).collect(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileStorable {
    pub path: PathBuf,
    pub tags: Vec<Uuid>,
}

impl Storable for File {
    type Storable = FileStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(FileStorable {
            path: self.path(),
            tags: self.tags.pin().iter().map(|t| t.id).collect(),
        })
    }
}

#[cfg(test)] //[!] Check 
mod tests {
    use std::time::Duration;

    use crate::service::FileSystem;
    use crate::service::ShelfDirKey;

    use super::*;
    use ebi_types::*;

    fn equal_fs(fs_left: &FileSystem, fs_right: &FileSystem) {
        let left_shelves_pin = fs_left.shelves.pin();
        let right_shelves_pin = fs_right.shelves.pin();
        let mut left_shelves: Vec<_> = left_shelves_pin.into_iter().collect();
        let mut right_shelves: Vec<_> = right_shelves_pin.into_iter().collect();
        left_shelves.sort_by_key(|s| s.id);
        right_shelves.sort_by_key(|s| s.id);
        let shelves_zipped = left_shelves.iter().zip(right_shelves.iter());

        for (s_l, s_r) in shelves_zipped {
            assert_eq!(s_l.root, s_r.root);
            assert_eq!(s_l.root_path, s_r.root_path);
            assert_eq!(s_l.id, s_r.id);
            assert_eq!(s_l.dirs, s_r.dirs)
        }

        let left_dirs_pin = fs_left.dirs.pin();
        let right_dirs_pin = fs_right.dirs.pin();
        let mut left_dirs: Vec<_> = left_dirs_pin.into_iter().collect();
        let mut right_dirs: Vec<_> = right_dirs_pin.into_iter().collect();
        left_dirs.sort_by_key(|d| d.id);
        right_dirs.sort_by_key(|d| d.id);

        let dirs_zipped = left_dirs.iter().zip(right_dirs.iter());

        for (d_l, d_r) in dirs_zipped {
            assert_eq!(d_l.id, d_r.id);
            assert_eq!(d_l.path, d_r.path);
            assert_eq!(d_l.files, d_r.files);

            let d_l_files = d_l.files.pin();
            let d_r_files = d_r.files.pin();

            for (f_l, f_r) in d_l_files.iter().zip(d_r_files.iter()) {
                assert_eq!(f_l.path, f_r.path);
                assert_eq!(f_l.tags, f_r.tags);

                let f_l_tags = f_l.tags.pin();
                let f_r_tags = f_r.tags.pin();

                for (l_t, r_t) in f_l_tags.iter().zip(f_r_tags.iter()) {
                    let (l_t, r_t) = (l_t.load_full(), r_t.load_full());
                    assert_eq!(l_t.parent, r_t.parent);
                    assert_eq!(l_t.name, r_t.name);
                    assert_eq!(l_t.priority, r_t.priority);
                }
            }

            assert_eq!(d_l.tags, d_r.tags);
            assert_eq!(d_l.dtags, d_r.dtags);

            let l_dtags = d_l.dtags.pin();
            let r_dtags = d_r.dtags.pin();

            for (l_t, r_t) in l_dtags.iter().zip(r_dtags.iter()) {
                let (l_t, r_t) = (l_t.load_full(), r_t.load_full());
                assert_eq!(l_t.parent, r_t.parent);
                assert_eq!(l_t.name, r_t.name);
                assert_eq!(l_t.priority, r_t.priority);
            }

            let l_tags = d_l.tags.pin();
            let r_tags = d_r.tags.pin();

            for ((l_t, l_files), (r_t, r_files)) in l_tags.iter().zip(r_tags.iter()) {
                let (l_t, r_t) = (l_t.load_full(), r_t.load_full());
                assert_eq!(l_t.parent, r_t.parent);
                assert_eq!(l_t.name, r_t.name);
                assert_eq!(l_t.priority, r_t.priority);

                let l_files = l_files.pin();
                let r_files = r_files.pin();

                for (f_l, f_r) in l_files.iter().zip(r_files.iter()) {
                    assert_eq!(f_l.path, f_r.path);
                    assert_eq!(f_l.tags, f_r.tags);

                    let f_l_tags = f_l.tags.pin();
                    let f_r_tags = f_r.tags.pin();

                    for (l_t, r_t) in f_l_tags.iter().zip(f_r_tags.iter()) {
                        let (l_t, r_t) = (l_t.load_full(), r_t.load_full());
                        assert_eq!(l_t.parent, r_t.parent);
                        assert_eq!(l_t.name, r_t.name);
                        assert_eq!(l_t.priority, r_t.priority);
                    }
                }
            }

            assert_eq!(d_l.parent.load_full(), d_r.parent.load_full());
            assert_eq!(d_l.subdirs, d_r.subdirs);
        }
    }

    #[tokio::test]
    async fn save_restore_db() {
        let test_path = std::env::temp_dir().join("ebi-test");
        let test_path = test_path.join("save-restore-db");
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();

        let test_path = test_path.join(test_path.join("test_shelf_dir"));
        let _ = std::fs::create_dir(test_path.clone());
        let test_subpath = test_path.join("test_subdir");
        let _ = std::fs::create_dir(test_subpath.clone());
        let _ = std::fs::File::create(test_subpath.join("file.txt"));

        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(test_path.clone()))
            .await
            .unwrap();
        let _dir_id_1 = fs
            .get_or_init_dir(ShelfDirKey::Id(s.id), test_subpath.clone())
            .await
            .unwrap();

        let tag = SharedRef::new_ref(
            Uuid::new_v4(),
            Tag {
                priority: 0,
                name: "test".to_string(),
                parent: None,
            },
        );
        let c_tag = SharedRef::new_ref(
            Uuid::new_v4(),
            Tag {
                priority: 0,
                name: "test".to_string(),
                parent: Some(tag.clone()),
            },
        );

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s.id), test_subpath.join("file.txt"), c_tag)
            .await
            .unwrap();
        let _ = fs
            .attach_dtag(ShelfDirKey::Id(s.id), test_subpath.clone(), tag)
            .await
            .unwrap();

        // crete new fs with empty db but same data
        let shelves = fs.shelves.clone();
        let dirs = fs.dirs.clone();
        fs.close();
        std::thread::sleep(Duration::from_millis(100));
        drop(fs);

        let new_db_path = test_path.join("new_database.redb");
        let _ = std::fs::remove_file(&new_db_path);
        let mut fs = FileSystem::new(&new_db_path).unwrap();
        fs.shelves = shelves;
        fs.dirs = dirs;

        let load_fs = FileSystem::full_load(&db_path).await.unwrap();

        equal_fs(&load_fs, &fs);

        let _ = std::fs::remove_file(&new_db_path);
        let _ = std::fs::remove_file(&db_path);
    }
}
