use crate::dir::ShelfDir;
use crate::file::File;
use crate::redb::{T_FILE, T_SHELF_DATA, T_SHELF_DIR};
use crate::service::FileSystem;
use crate::shelf::ShelfData;
use crossbeam_channel::{Receiver, RecvError, Sender, unbounded};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::Storable;
use ebi_types::{FileId, ImmutRef, get_file_id};
use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
use notify::{Error, Event, EventKind, RecommendedWatcher, Watcher};
use redb::ReadableTable;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct _Config {
    recursive: bool,
}

#[derive(Debug)]
pub enum Signal {
    Close,
    Event(FileId, Result<Event, Error>),
}

impl PartialEq for Signal {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Signal::Close, Signal::Close) | (Signal::Event(_, _), Signal::Event(_, _))
        )
    }
}

impl Signal {
    pub fn is_close(&self) -> bool {
        *self == Signal::Close
    }
}

#[derive(Clone, Debug)]
pub struct Channel {
    pub tx: Sender<Signal>,
    pub rx: Receiver<Signal>,
}
impl Channel {
    pub fn new() -> Self {
        let (tx, rx) = unbounded();
        Channel { tx, rx }
    }
}

impl Default for Channel {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ShelfWatcher(RecommendedWatcher);

impl ShelfWatcher {
    pub fn new(id: FileId, path: &Path, tx: Sender<Signal>) -> Self {
        let event_handler = move |result: Result<Event, Error>| {
            let tx = tx.clone();

            if let Err(_e) = tx.send(Signal::Event(id, result)) {
                //eprintln!("Error sending event result: {:?}", e);
            }
        };

        let mut watcher =
            RecommendedWatcher::new(event_handler, notify::Config::default()).unwrap();
        watcher
            .watch(path, notify::RecursiveMode::Recursive)
            .unwrap();
        ShelfWatcher(watcher)
    }
}

impl FileSystem {
    pub fn handle_new_dir(
        &self,
        shelf: &ImmutRef<ShelfData, FileId>,
        dir: &ImmutRef<ShelfDir, FileId>,
        p_id: &FileId,
        new_path: &Path,
    ) {
        let dirs = self.dirs.pin();
        let p_ref = dir.parent.load_full().as_ref().clone();
        let p = p_ref.and_then(|p| if p.id != *p_id { None } else { p.upgrade() });

        let parent = match p {
            Some(p) => p,
            None => {
                let new_p = dirs.get(p_id);
                if let Some(new_p) = new_p {
                    dir.parent.store(Some(new_p.downgrade()).into());
                    new_p.data_ref().clone()
                } else {
                    return;
                }
            }
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

        dir.path.update(new_path);
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
        let parent = if let Some(parent) = self.dirs.pin().get(p_id).cloned() {
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
    pub fn handle_new_file(&self, file: &ImmutRef<File, FileId>, p_id: &FileId, new_path: &Path) {
        let write_txn = self.db.begin_write().unwrap();
        let mut dir_table = write_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();
        let mut file_table = write_txn
            .open_table(T_FILE)
            .map_err(|_| ReturnCode::InternalStateError)
            .unwrap();

        if let Some(dir) = self.dirs.pin().get(p_id) {
            dir.files.pin().insert(file.clone());
            let mut dir_e = dir_table.get(dir.id).unwrap().unwrap().value().clone();
            dir_e.0.files.push(file.id);
            dir_table.insert(dir.id, dir_e).unwrap();
        } else if let Some(dir) = self.orphan_dirs.pin().get(p_id) {
            dir.files.pin().insert(file.clone());
        }
        self.orphan_files.pin().remove(&file.id);
        file.path.update(new_path);
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
        let dirs = shelf.dirs.pin();

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

            dirs.remove(&dir.id);

            let mut shelf_e = shelf_table.get(shelf.id).unwrap().unwrap().value().clone();
            shelf_e.0.dirs.retain(|id| *id != dir.id);
            shelf_table.insert(shelf.id, shelf_e).unwrap();

            self.dirs.pin().remove(&dir.id);
            self.orphan_dirs.pin().insert(dir.clone());
            dir_table.remove(dir.id).unwrap();
        }
    }
}

#[derive(Debug)]
enum Entity {
    File,
    Dir,
    Unknown,
}

#[derive(Debug)]
enum Operation {
    Add(Entity),
    Remove(Entity),
}

#[derive(Debug)]
struct Task {
    op: Operation,
    shelf_id: FileId,
    path: PathBuf,
    new_path: PathBuf,
}

impl Task {
    fn priority(&self) -> u8 {
        match self.op {
            Operation::Remove(Entity::File) => 0,
            Operation::Remove(Entity::Unknown) => 1,
            Operation::Remove(Entity::Dir) => 2,
            Operation::Add(Entity::Dir) => 3,
            Operation::Add(Entity::Unknown) => 4,
            Operation::Add(Entity::File) => 5,
        }
    }
}

impl FileSystem {
    fn process_task(&self, mut queue: VecDeque<Task>) -> Option<()> {
        let mapped_ids = self.mapped_ids.pin();
        let orphan_files = self.orphan_files.pin();
        let orphan_dirs = self.orphan_dirs.pin();
        let files = self.files.pin();
        let dirs = self.dirs.pin();

        while let Some(task) = queue.pop_front() {
            dbg!(&task);
            let shelf = self.shelves.pin().get(&task.shelf_id).cloned()?;
            let parent_path = task.path.parent().map(|f| f.to_path_buf())?; // [TODO] handle root
            let parent_id = mapped_ids.get(&parent_path).cloned()?;
            match task.op {
                Operation::Add(entity) => {
                    let path_id = get_file_id(task.path).ok()?;
                    match entity {
                        Entity::File => {
                            let file = orphan_files
                                .get(&path_id)
                                .cloned()
                                .or(files.get(&path_id).cloned())?;
                            self.handle_new_file(&file, &parent_id, &task.new_path);
                        }
                        Entity::Dir => {
                            let dir = orphan_dirs.get(&path_id).cloned()?;
                            self.handle_new_dir(&shelf, &dir, &parent_id, &task.new_path);
                        }
                        Entity::Unknown => {
                            if let Some(dir) = orphan_dirs.get(&path_id).cloned() {
                                self.handle_new_dir(&shelf, &dir, &parent_id, &task.new_path);
                            } else if let Some(file) = orphan_files
                                .get(&path_id)
                                .cloned()
                                .or(files.get(&path_id).cloned())
                            {
                                self.handle_new_file(&file, &parent_id, &task.new_path);
                            }
                        }
                    }
                }
                Operation::Remove(entity) => {
                    let path_id = mapped_ids.get(&task.path)?;
                    match entity {
                        Entity::File => {
                            let file = dirs
                                .get(&parent_id)
                                .and_then(|p| p.files.pin().get(path_id).cloned())?;
                            self.handle_del_file(&file, &parent_id);
                        }
                        Entity::Dir => {
                            let dir = dirs.get(path_id).cloned()?;
                            self.handle_del_dir(&shelf, &dir, &parent_id);
                        }
                        Entity::Unknown => {
                            if let Some(dir) = dirs.get(path_id).cloned() {
                                self.handle_del_dir(&shelf, &dir, &parent_id);
                            } else if let Some(file) = dirs
                                .get(&parent_id)
                                .and_then(|p| p.files.pin().get(path_id).cloned())
                            {
                                self.handle_del_file(&file, &parent_id);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    fn preprocess_event(&self, signal: Result<Signal, RecvError>) -> Option<Task> {
        match signal {
            Ok(Signal::Event(id, Ok(event))) => {
                let path = event.paths.first()?;
                match event.kind {
                    // access updated
                    EventKind::Access(_) => None,
                    EventKind::Modify(kind) => {
                        match kind {
                            // file contents
                            ModifyKind::Data(_kind) => None,
                            // rename and moving
                            ModifyKind::Name(rename_mode) => match rename_mode {
                                RenameMode::From => Some(Task {
                                    op: Operation::Remove(Entity::Unknown),
                                    shelf_id: id,
                                    new_path: path.clone(),
                                    path: path.clone(),
                                }),
                                RenameMode::To => Some(Task {
                                    op: Operation::Add(Entity::Unknown),
                                    shelf_id: id,
                                    path: path.clone(),
                                    new_path: path.clone(),
                                }),
                                RenameMode::Both => None,
                                _ => None,
                            },
                            ModifyKind::Metadata(_) => None,
                            _ => None,
                        }
                    }
                    EventKind::Create(CreateKind::File) => Some(Task {
                        op: Operation::Add(Entity::File),
                        shelf_id: id,
                        path: path.clone(),
                        new_path: path.clone(),
                    }),
                    EventKind::Create(CreateKind::Folder) => Some(Task {
                        op: Operation::Add(Entity::Dir),
                        shelf_id: id,
                        path: path.clone(),
                        new_path: path.clone(),
                    }),
                    EventKind::Remove(RemoveKind::File) => Some(Task {
                        op: Operation::Remove(Entity::File),
                        shelf_id: id,
                        path: path.clone(),
                        new_path: path.clone(),
                    }),
                    EventKind::Remove(RemoveKind::Folder) => Some(Task {
                        op: Operation::Remove(Entity::Dir),
                        shelf_id: id,
                        path: path.clone(),
                        new_path: path.clone(),
                    }),
                    _ => None,
                }
            }
            Ok(Signal::Event(_, Err(_))) => None,
            Ok(Signal::Close) => None,
            Err(_) => None,
        }
    }
}

impl FileSystem {
    pub(crate) fn event_handler(&self) {
        let fs = self.clone();
        let rx = self.event_channel.rx.clone();

        std::thread::spawn(move || {
            loop {
                let signal = rx.recv();
                let mut close = signal.as_ref().map(|s| s.is_close()).unwrap_or(false);

                let mut fn_queue = VecDeque::<Task>::new();
                if let Some(task) = fs.preprocess_event(signal) {
                    fn_queue.push_front(task);
                }
                for _ in 0..3 {
                    let start = Instant::now();
                    let mut received = Instant::now();
                    while received.saturating_duration_since(start) < Duration::from_millis(10) {
                        let signal = rx
                            .recv_timeout(Duration::from_millis(10))
                            .map_err(|_| RecvError);
                        close |= signal.as_ref().map(|s| s.is_close()).unwrap_or(false);
                        if let Some(task) = fs.preprocess_event(signal) {
                            fn_queue.push_front(task);
                        }
                        received = Instant::now();
                    }
                }
                let fs = fs.clone();
                fn_queue.make_contiguous().sort_by_key(|t| t.priority());
                std::thread::spawn(move || fs.process_task(fn_queue));
                if close {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::service::FileSystem;
    use crate::service::ShelfDirKey;
    use ebi_types::tag::Tag;
    use ebi_types::*;

    #[tokio::test]
    async fn remove_file() {
        let test_path = std::env::temp_dir().join("ebi-test");
        let test_path = test_path.join("remove-file");
        let _ = std::fs::create_dir_all(test_path.clone());

        let db_path = test_path.join("database.redb");
        let shelf_path = test_path.join(test_path.join("test_shelf_path"));

        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();
        let _ = std::fs::create_dir(shelf_path.clone());
        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(shelf_path.clone()))
            .await
            .unwrap();
        let tag = SharedRef::new_ref(Tag {
            priority: 0,
            name: "test".to_string(),
            parent: None,
        });
        let test_f_path = shelf_path.join("test_file");
        let _file = std::fs::File::create(test_f_path.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s.id), test_f_path.clone(), tag.clone())
            .await
            .unwrap();

        let mapped_ids = fs.mapped_ids.pin();
        let f_id = mapped_ids.get(&test_f_path).unwrap();
        let _ = std::fs::remove_file(test_f_path.clone());
        std::thread::sleep(Duration::from_millis(50));

        assert!(
            fs.dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_none()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_some());
        let _ = std::fs::remove_dir(test_path.clone());
    }

    #[tokio::test]
    async fn move_file() {
        let test_path = std::env::temp_dir().join("ebi-test");
        let test_path = test_path.join("move-file");
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let shelf_path = test_path.join(test_path.join("test_shelf_path"));

        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();
        let _ = std::fs::create_dir(shelf_path.clone());
        let s = fs
            .get_or_init_shelf(ShelfDirKey::Path(shelf_path.clone()))
            .await
            .unwrap();
        let tag = SharedRef::new_ref(Tag {
            priority: 0,
            name: "test".to_string(),
            parent: None,
        });
        let test_f_path = shelf_path.join("file.txt");
        let _file = std::fs::File::create(test_f_path.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s.id), test_f_path.clone(), tag.clone())
            .await
            .unwrap();

        let mapped_ids = fs.mapped_ids.pin();
        let f_id = mapped_ids.get(&test_f_path).unwrap();

        assert!(
            fs.dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_some()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_none());
        let new_path = test_path.join("renamed_test_file");

        let _ = std::fs::rename(test_f_path.clone(), new_path.clone());
        std::thread::sleep(Duration::from_millis(50));

        assert!(
            fs.dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_none()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_some());

        let _ = std::fs::rename(new_path.clone(), test_f_path.clone());
        std::thread::sleep(Duration::from_millis(50));

        assert!(
            fs.dirs
                .pin()
                .get(&s.id)
                .unwrap()
                .files
                .pin()
                .get(f_id)
                .is_some()
        );
        assert!(fs.orphan_files.pin().get(f_id).is_none());
    }

    #[tokio::test]
    async fn move_between_shelves() {
        let test_path = std::env::temp_dir().join("ebi-test");
        let test_path = test_path.join("move-between-shelves");
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let shelf_path1 = test_path.join(test_path.join("test_shelf_path1"));
        let shelf_path2 = test_path.join(test_path.join("test_shelf_path2"));

        let _ = std::fs::remove_file(&db_path);
        let mut fs = FileSystem::new(&db_path).unwrap();
        let _ = std::fs::create_dir(shelf_path1.clone());
        let _ = std::fs::create_dir(shelf_path2.clone());
        let s1 = fs
            .get_or_init_shelf(ShelfDirKey::Path(shelf_path1.clone()))
            .await
            .unwrap();
        let s2 = fs
            .get_or_init_shelf(ShelfDirKey::Path(shelf_path2.clone()))
            .await
            .unwrap();
        let tag = SharedRef::new_ref(Tag {
            priority: 0,
            name: "test".to_string(),
            parent: None,
        });

        let test_f_path1 = shelf_path1.join("file1.txt");
        let _ = std::fs::File::create(test_f_path1.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s1.id), test_f_path1.clone(), tag.clone())
            .await
            .unwrap();

        let test_f_path2 = shelf_path2.join("file2.txt");
        let _ = std::fs::File::create(test_f_path2.clone()).unwrap();

        let _ = fs
            .attach_tag(ShelfDirKey::Id(s2.id), test_f_path2.clone(), tag.clone())
            .await
            .unwrap();

        let mapped_ids = fs.mapped_ids.pin();
        let f_id1 = mapped_ids.get(&test_f_path1).unwrap();
        let f_id2 = mapped_ids.get(&test_f_path2).unwrap();

        let temp_path = test_path.join("tmp.txt");
        let _ = std::fs::rename(test_f_path1.clone(), temp_path.clone());
        let _ = std::fs::rename(test_f_path2.clone(), test_f_path1.clone());
        let _ = std::fs::rename(temp_path.clone(), test_f_path2.clone());
        std::thread::sleep(Duration::from_millis(50));

        assert!(
            fs.dirs
                .pin()
                .get(&s1.id)
                .unwrap()
                .files
                .pin()
                .get(f_id2)
                .is_some()
        );
        assert!(
            fs.dirs
                .pin()
                .get(&s1.id)
                .unwrap()
                .files
                .pin()
                .get(f_id1)
                .is_none()
        );
        assert!(
            fs.dirs
                .pin()
                .get(&s2.id)
                .unwrap()
                .files
                .pin()
                .get(f_id1)
                .is_some()
        );
        assert!(
            fs.dirs
                .pin()
                .get(&s2.id)
                .unwrap()
                .files
                .pin()
                .get(f_id2)
                .is_none()
        );
        assert!(fs.orphan_files.pin().get(f_id1).is_none());
        assert!(fs.orphan_files.pin().get(f_id2).is_none());
    }
}
