use std::borrow::Borrow;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::dir::ShelfDir;
use crate::file::File;
use crate::redb::{T_FILE, T_SHELF_DATA, T_SHELF_DIR};
use crate::service::FileSystem;
use crate::shelf::ShelfData;
use crossbeam_channel::{Receiver, Sender, unbounded};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::{FileId, ImmutRef, get_file_id};
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Error, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use redb::ReadableTable;

struct _Config {
    recursive: bool,
}

#[derive(Clone, Debug)]
pub struct ShelfWatcher {
    pub id: FileId,
    pub inner: Arc<RecommendedWatcher>,
    pub channel: Channel,
}

#[derive(Clone, Debug)]
pub struct Channel {
    pub tx: Sender<Signal>,
    pub rx: Receiver<Signal>,
}

pub enum Signal {
    Close,
    Event(Result<Event, Error>),
}

impl Eq for ShelfWatcher {}

impl PartialEq for ShelfWatcher {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for ShelfWatcher {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl Borrow<FileId> for ShelfWatcher {
    fn borrow(&self) -> &FileId {
        &self.id
    }
}

impl ShelfWatcher {
    pub fn new(id: FileId, path: &Path) -> Result<Self, Error> {
        let (tx, rx) = unbounded();
        let tx_i = tx.clone();
        let event_handler = move |result: Result<Event, Error>| {
            let tx = tx_i.clone();

            if let Err(_e) = tx.send(Signal::Event(result)) {
                //eprintln!("Error sending event result: {:?}", e);
            }
        };

        let mut watcher = RecommendedWatcher::new(event_handler, notify::Config::default())?;
        watcher.watch(path, RecursiveMode::Recursive)?;
        let inner = Arc::new(watcher);
        Ok(ShelfWatcher {
            id,
            inner,
            channel: Channel { tx, rx },
        })
    }
}

impl FileSystem {
    pub fn handle_new_dir(
        &self,
        shelf: &ImmutRef<ShelfData, FileId>,
        dir: &ImmutRef<ShelfDir, FileId>,
        p_id: &FileId,
    ) {
        let dirs = self.shelf_dirs.pin();
        let Some(parent) = ({
            let p_ref = dir.parent.load_full().as_ref().clone();
            let p = p_ref.and_then(|p| p.upgrade());
            match p {
                Some(p) => Some(p),
                None => dirs.get(p_id).map(|f| f.data_ref()).cloned(),
            }
        }) else {
            return;
        };

        let write_txn = self.db.begin_write().unwrap();
        let mut dir_table = write_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let mut shelf_table = write_txn
            .open_table(T_SHELF_DATA)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();

        dir_table.insert(dir.id, dir.to_storable()).unwrap();
        parent.subdirs.pin().insert(dir.downgrade());
        let mut p_e = dir_table.get(p_id).unwrap().unwrap().value().clone();
        p_e.0.subdirs.push(dir.id);
        dir_table.insert(p_id, p_e).unwrap();
        dirs.insert(dir.clone());
        self.orphan_dirs.pin().remove(dir);
        let mut shelf_e = shelf_table.get(shelf.id).unwrap().unwrap().value().clone();
        shelf_e.0.dirs.push(dir.id);
        shelf_table.insert(shelf.id, shelf_e).unwrap();
        shelf.dirs.pin().insert(dir.downgrade());
    }
    pub fn handle_del_file(&self, file: &ImmutRef<File, FileId>, p_id: &FileId) {
        let write_txn = self.db.begin_write().unwrap();
        let mut dir_table = write_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let mut file_table = write_txn
            .open_table(T_FILE)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let parent = if let Some(parent) = self.shelf_dirs.pin().get(p_id).cloned() {
            let mut p_e = dir_table.get(parent.id).unwrap().unwrap().value().clone();
            p_e.0.files.retain(|i| *i != file.id);
            dir_table.insert(parent.id, p_e).unwrap();
            parent
        } else {
            self.orphan_dirs.pin().get(p_id).cloned().unwrap()
        };

        parent.files.pin().remove(&file.id);
        file_table.remove(file.id).unwrap();
        self.orphan_files.pin().insert(file.clone());
    }
    pub fn handle_new_file(&self, file: &ImmutRef<File, FileId>, p_id: &FileId) {
        let write_txn = self.db.begin_write().unwrap();
        let mut dir_table = write_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let mut file_table = write_txn
            .open_table(T_FILE)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        if let Some(dir) = self.shelf_dirs.pin().get(p_id) {
            dir.files.pin().insert(file.clone());
            let mut dir_e = dir_table.get(dir.id).unwrap().unwrap().value().clone();
            dir_e.0.files.push(file.id);
            dir_table.insert(dir.id, dir_e).unwrap();
        } else if let Some(dir) = self.orphan_dirs.pin().get(p_id) {
            dir.files.pin().insert(file.clone());
        }
        self.orphan_files.pin().remove(&file.id);
        file_table
            .insert(file.id, file.to_storable())
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
    }

    pub fn handle_del_dir(
        &self,
        shelf: &ImmutRef<ShelfData, FileId>,
        dir: &ImmutRef<ShelfDir, FileId>,
        _p_id: &FileId,
    ) {
        let write_txn = self.db.begin_write().unwrap();
        let mut dir_table = write_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let mut shelf_table = write_txn
            .open_table(T_SHELF_DATA)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let shelf_dirs = shelf.dirs.pin();

        if dir.path == shelf.root_path {
            unreachable!();
        } else {
            let parent = dir.parent.load_full().as_ref().clone().unwrap();
            let p_id = parent.id;
            let parent = parent.upgrade().unwrap();
            parent.subdirs.pin().remove(&dir.id);
            let mut p_e = dir_table.get(p_id).unwrap().unwrap().value().clone();
            p_e.0.subdirs.retain(|id| *id != dir.id);
            dir_table.insert(p_id, p_e).unwrap();

            shelf_dirs.remove(&dir.id);

            let mut shelf_e = shelf_table.get(shelf.id).unwrap().unwrap().value().clone();
            shelf_e.0.dirs.retain(|id| *id != dir.id);
            shelf_table.insert(shelf.id, shelf_e).unwrap();

            self.shelf_dirs.pin().remove(&dir.id);
            self.orphan_dirs.pin().insert(dir.clone());
            dir_table.remove(dir.id).unwrap();
        }
    }
    fn init_event_path(&self, event_kind: EventKind, path: &PathBuf) -> Option<(FileId, FileId)> {
        let mapped_ids = self.mapped_ids.pin();

        let path_id = match event_kind {
            EventKind::Create(_) | EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                get_file_id(path).ok()
            }
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                mapped_ids.get(path).copied()
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => mapped_ids.get(path).copied(),
            _ => None,
        }?;

        let parent_path = path.parent().map(|f| f.to_path_buf())?; // [TODO] handle the case when
                                                                   // path is already root
        let p_id = mapped_ids.get(&parent_path).cloned()?;
        Some((path_id, p_id))
    }
    fn init_dir(
        &self,
        dir_id: &FileId,
        event_kind: EventKind,
    ) -> Option<ImmutRef<ShelfDir, FileId>> {
        match event_kind {
            EventKind::Create(CreateKind::Folder)
            | EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                self.orphan_dirs.pin().get(dir_id).cloned()
            }
            EventKind::Remove(RemoveKind::Folder)
            | EventKind::Modify(ModifyKind::Name(RenameMode::From)) => {
                self.shelf_dirs.pin().get(dir_id).cloned()
            }
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => {
                self.shelf_dirs.pin().get(dir_id).cloned()
            }
            _ => None,
        }
    }
    fn init_file(
        &self,
        file_id: &FileId,
        event_kind: EventKind,
        p_id: &FileId,
    ) -> Option<ImmutRef<File, FileId>> {
        match event_kind {
            EventKind::Create(CreateKind::File)
            | EventKind::Modify(ModifyKind::Name(RenameMode::To)) => {
                self.orphan_files.pin().get(file_id).cloned()
            }
            EventKind::Remove(RemoveKind::File)
            | EventKind::Modify(ModifyKind::Name(RenameMode::From)) => self
                .shelf_dirs
                .pin()
                .get(p_id)
                .and_then(|p| p.files.pin().get(file_id).cloned()),
            _ => None,
        }
    }
}

impl FileSystem {
    pub fn watch_shelf(&self, id: FileId) -> Result<(), ReturnCode> {
        let local_shelves = self.local_shelves.pin();
        let Some(shelf) = local_shelves.get(&id) else {
            return Err(ReturnCode::ShelfNotFound);
        };
        let watchers = self.watchers.pin();

        let Some(watcher) = watchers.get(&id) else {
            return Err(ReturnCode::InternalStateError);
        };

        let watcher = watcher.clone();
        let shelf = shelf.clone();
        let rx = watcher.channel.rx;
        let fs = self.clone();

        std::thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(Signal::Event(Ok(event))) => {
                        match event.kind {
                            EventKind::Access(_) => {} // access updated
                            EventKind::Modify(kind) => {
                                match kind {
                                    ModifyKind::Data(_kind) => {} // file content
                                    ModifyKind::Name(rename_mode) => {
                                        let Some(path) = event.paths.first() else {
                                            continue;
                                        };

                                        let Some((path_id, p_id)) =
                                            fs.init_event_path(event.kind, path)
                                        else {
                                            continue;
                                        };

                                        if let Some(dir) = fs.init_dir(&path_id, event.kind) {
                                            match rename_mode {
                                                RenameMode::From => {
                                                    fs.handle_del_dir(&shelf, &dir, &p_id);
                                                }
                                                RenameMode::To => {
                                                    fs.handle_new_dir(&shelf, &dir, &p_id);
                                                }
                                                RenameMode::Both => {}
                                                _ => {}
                                            }
                                        } else if let Some(file) =
                                            fs.init_file(&path_id, event.kind, &p_id)
                                        {
                                            match rename_mode {
                                                RenameMode::From => {
                                                    fs.handle_del_file(&file, &p_id);
                                                }
                                                RenameMode::To => {
                                                    fs.handle_new_file(&file, &p_id);
                                                }
                                                RenameMode::Both => {}
                                                _ => {}
                                            }
                                        }
                                    }
                                    ModifyKind::Metadata(_) => {}
                                    _ => {}
                                }
                            }
                            k @ (EventKind::Create(_) | EventKind::Remove(_)) => {
                                for path in event.paths {
                                    let Some((path_id, p_id)) =
                                        fs.init_event_path(event.kind, &path)
                                    else {
                                        continue;
                                    };

                                    match k {
                                        EventKind::Create(CreateKind::File) => {
                                            let Some(file) =
                                                fs.init_file(&path_id, event.kind, &p_id)
                                            else {
                                                continue;
                                            };
                                            fs.handle_new_file(&file, &p_id);
                                        }
                                        EventKind::Create(CreateKind::Folder) => {
                                            let Some(dir) = fs.init_dir(&path_id, event.kind)
                                            else {
                                                continue;
                                            };
                                            fs.handle_new_dir(&shelf, &dir, &p_id);
                                        }
                                        EventKind::Remove(RemoveKind::File) => {
                                            let Some(file) =
                                                fs.init_file(&path_id, event.kind, &p_id)
                                            else {
                                                continue;
                                            };
                                            fs.handle_del_file(&file, &p_id);
                                        }
                                        EventKind::Remove(RemoveKind::Folder) => {
                                            let Some(dir) = fs.init_dir(&path_id, event.kind)
                                            else {
                                                continue;
                                            };
                                            fs.handle_del_dir(&shelf, &dir, &p_id);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Ok(Signal::Event(Err(_))) => {}
                    Ok(Signal::Close) => {
                        return;
                    }
                    Err(_) => {
                        return;
                    }
                }
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::service::FileSystem;
    use crate::service::ShelfDirKey;
    use ebi_types::tag::Tag;
    use ebi_types::*;

    use rand::Rng;

    #[tokio::test]
    async fn remove_file() {
        let tmp_path = std::env::temp_dir().join("ebi-tests");
        let _ = std::fs::create_dir(tmp_path.clone());
        let n: u32 = rand::thread_rng().gen_range(100_000..=999_999);

        let db_path = tmp_path.join(format!("dummy-{n}.redb"));
        let test_shelf_path = tmp_path.join(tmp_path.join(format!("testdir-{n}")));

        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();
        let _ = std::fs::create_dir(test_shelf_path.clone()).unwrap();
        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(test_shelf_path.clone()))
            .await
            .unwrap();
        let tag = SharedRef::new_ref(Tag {
            priority: 0,
            name: "test".to_string(),
            parent: None,
        });
        let test_f_path = test_shelf_path.join("test_file");
        let _file = std::fs::File::create(test_f_path.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s.id), test_f_path.clone(), tag.clone())
            .await
            .unwrap();

        let mapped_ids = fs.mapped_ids.pin();
        let f_id = mapped_ids.get(&test_f_path).unwrap();
        let _ = std::fs::remove_file(test_f_path.clone());
        let _ = std::fs::remove_dir(tmp_path.clone());
        std::thread::sleep(Duration::from_millis(10));

        assert!(
            fs.shelf_dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_none()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_some());
    }
    #[tokio::test]
    async fn moving_file() {
        let tmp_path = std::env::temp_dir().join("ebi-tests");
        let _ = std::fs::create_dir(tmp_path.clone());
        let n: u32 = rand::thread_rng().gen_range(100_000..=999_999);

        let db_path = tmp_path.join(format!("dummy-{n}.redb"));
        let test_shelf_path = tmp_path.join(tmp_path.join(format!("testdir-{n}")));

        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();
        let _ = std::fs::create_dir(test_shelf_path.clone()).unwrap();
        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(test_shelf_path.clone()))
            .await
            .unwrap();
        let tag = SharedRef::new_ref(Tag {
            priority: 0,
            name: "test".to_string(),
            parent: None,
        });
        let test_f_path = test_shelf_path.join("test_file");
        let _file = std::fs::File::create(test_f_path.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s.id), test_f_path.clone(), tag.clone())
            .await
            .unwrap();

        let mapped_ids = fs.mapped_ids.pin();
        let f_id = mapped_ids.get(&test_f_path).unwrap();

        assert!(
            fs.shelf_dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_some()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_none());
        let new_path = tmp_path.join("renamed_test_file");

        let _ = std::fs::rename(test_f_path.clone(), new_path.clone());
        std::thread::sleep(Duration::from_millis(10));

        assert!(
            fs.shelf_dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_none()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_some());
    }
}
