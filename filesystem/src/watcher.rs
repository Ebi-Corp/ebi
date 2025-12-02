use std::borrow::Borrow;
use std::hash::Hash;
use std::path::Path;
use std::sync::Arc;

use crate::redb::{T_FILE, T_SHELF_DATA, T_SHELF_DIR};
use crate::service::FileSystem;
use ebi_proto::rpc::ReturnCode;
use ebi_types::FileId;
use ebi_types::redb::Storable;
use notify::event::{CreateKind, RemoveKind};
use notify::{Error, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use redb::ReadableTable;
use crossbeam_channel::{Receiver, Sender, unbounded};

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
    pub rx: Receiver<Signal>
}

pub enum Signal {
    Close,
    Event(Result<Event, Error>)
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
            channel: Channel {
                tx,
                rx
            },
        })
    }
}

impl FileSystem {
    pub async fn watch_shelf(&self, id: FileId) -> Result<(), ReturnCode> {
        let db = self.db.clone();
        let dirs = self.shelf_dirs.clone();
        let orphan_files = self.orphan_files.clone();
        let orphan_dirs = self.orphan_dirs.clone();
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
        let rx = watcher.channel.rx.clone();

        std::thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(Signal::Event(Ok(mut event))) => {
                        match event.kind {
                            EventKind::Any => {}       // catchall
                            EventKind::Access(_) => {} // access updated
                            EventKind::Modify(_kind) => {
                                // name or metadata change
                            }
                            EventKind::Create(kind) => {
                                if kind != CreateKind::File && kind != CreateKind::Folder {
                                    continue;
                                }
                                let dirs = dirs.pin();
                                let orphan_files = orphan_files.pin();
                                let orphan_dirs = orphan_dirs.pin();
                                let write_txn = db.begin_write().unwrap();
                                let mut dir_table = write_txn
                                    .open_table(T_SHELF_DIR)
                                    .map_err(|_| ReturnCode::InternalStateError)
                                    .unwrap();
                                let mut shelf_table = write_txn
                                    .open_table(T_SHELF_DATA)
                                    .map_err(|_| ReturnCode::InternalStateError)
                                    .unwrap();

                                if kind == CreateKind::File {
                                    let mut file_table = write_txn
                                        .open_table(T_FILE)
                                        .map_err(|_| ReturnCode::InternalStateError)
                                        .unwrap();
                                    for path in event.paths {
                                        if let Some(file) = orphan_files.get(&path) {
                                            let parent_path = path.parent().unwrap().to_path_buf();
                                            if let Some(dir) = dirs.get(&parent_path) {
                                                dir.files.pin().insert(file.clone());
                                                let mut dir_e = dir_table
                                                    .get(dir.id)
                                                    .unwrap()
                                                    .unwrap()
                                                    .value()
                                                    .clone();
                                                dir_e.0.files.push(file.id);
                                                dir_table.insert(dir.id, dir_e).unwrap();
                                            } else if let Some(dir) = orphan_dirs.get(&parent_path)
                                            {
                                                // if the file has been added but it's part of
                                                // an orphaned dirs, it is possible that the
                                                // dir is being moved back to the shelf
                                                dir.files.pin().insert(file.clone());
                                            }
                                            file_table
                                                .insert(file.id, file.to_storable())
                                                .map_err(|_| ReturnCode::InternalStateError)
                                                .unwrap();
                                        }
                                    }
                                } else if kind == CreateKind::Folder {
                                    let shelf_dirs = shelf.dirs.pin();
                                    event.paths.sort_by(|a, b| {
                                        let da = a.components().count();
                                        let db = b.components().count();
                                        da.cmp(&db)
                                    });
                                    // we need to sort by components because we need to process
                                    // parents before of subdirs.

                                    for path in &event.paths {
                                        if let Some(dir) = orphan_dirs.get(path) {
                                            let parent_path = path.parent().unwrap().to_path_buf();
                                            let (parent, p_id) = {
                                                if let Some(p_ref) = dir.parent.load_full().as_ref()
                                                    && let (Some(parent), p_id) =
                                                        (p_ref.upgrade(), p_ref.id)
                                                {
                                                    (parent.clone(), p_id)
                                                } else if let Some(parent) = dirs.get(&parent_path)
                                                {
                                                    dir.parent
                                                        .store(Some(parent.downgrade()).into());
                                                    (parent.data_ref().clone(), parent.id)
                                                } else {
                                                    continue;
                                                }
                                            };
                                            dir_table.insert(dir.id, dir.to_storable()).unwrap();
                                            parent.subdirs.pin().insert(dir.downgrade());
                                            let mut p_e = dir_table
                                                .get(p_id)
                                                .unwrap()
                                                .unwrap()
                                                .value()
                                                .clone();
                                            p_e.0.subdirs.push(dir.id);
                                            dir_table.insert(p_id, p_e).unwrap();
                                            dirs.insert(dir.clone());
                                            orphan_dirs.remove(dir);
                                            let mut shelf_e = shelf_table
                                                .get(shelf.id)
                                                .unwrap()
                                                .unwrap()
                                                .value()
                                                .clone();
                                            shelf_e.0.dirs.push(dir.id);
                                            shelf_table.insert(shelf.id, shelf_e).unwrap();
                                            shelf_dirs.insert(dir.downgrade());
                                        }
                                    }
                                }
                            }
                            EventKind::Remove(kind) => {
                                if kind != RemoveKind::File && kind != RemoveKind::Folder {
                                    continue;
                                }
                                let dirs = dirs.pin();
                                let orphan_dirs = orphan_dirs.pin();
                                let write_txn = db.begin_write().unwrap();
                                let mut dir_table = write_txn
                                    .open_table(T_SHELF_DIR)
                                    .map_err(|_| ReturnCode::InternalStateError)
                                    .unwrap();
                                let mut shelf_table = write_txn
                                    .open_table(T_SHELF_DATA)
                                    .map_err(|_| ReturnCode::InternalStateError)
                                    .unwrap();

                                if kind == RemoveKind::File {
                                    let mut file_table = write_txn
                                        .open_table(T_FILE)
                                        .map_err(|_| ReturnCode::InternalStateError)
                                        .unwrap();

                                    for path in &event.paths {
                                        let parent_path = path.parent().unwrap().to_path_buf();
                                        if let Some(parent) = dirs.get(&parent_path) {
                                            if let Some(file) = parent.files.pin().get(path) {
                                                let mut p_e = dir_table
                                                    .get(parent.id)
                                                    .unwrap()
                                                    .unwrap()
                                                    .value()
                                                    .clone();
                                                p_e.0.files.retain(|i| *i != file.id);
                                                dir_table.insert(parent.id, p_e).unwrap();

                                                file_table.remove(file.id).unwrap();
                                            }
                                        } else if let Some(parent) = orphan_dirs.get(&parent_path)
                                            && let Some(file) = parent.files.pin().get(path)
                                        {
                                            file_table.remove(file.id).unwrap();
                                        }
                                    }
                                } else if kind == RemoveKind::Folder {
                                    let shelf_dirs = shelf.dirs.pin();

                                    // we need to sort by components because we need to process
                                    // parents before of subdirs.
                                    event.paths.sort_by(|a, b| {
                                        let da = a.components().count();
                                        let db = b.components().count();
                                        da.cmp(&db)
                                    });

                                    for path in &event.paths {
                                        if let Some(dir) = dirs.get(path) {
                                            if dir.id == shelf.root.id {
                                                // we exit here
                                            }

                                            let parent =
                                                dir.parent.load_full().as_ref().clone().unwrap();
                                            let p_id = parent.id;
                                            let parent = parent.upgrade().unwrap();
                                            parent.subdirs.pin().remove(&dir.id);
                                            let mut p_e = dir_table
                                                .get(p_id)
                                                .unwrap()
                                                .unwrap()
                                                .value()
                                                .clone();
                                            p_e.0.subdirs.retain(|id| *id != dir.id);
                                            dir_table.insert(p_id, p_e).unwrap();

                                            shelf_dirs.remove(&dir.id);

                                            let mut shelf_e = shelf_table
                                                .get(shelf.id)
                                                .unwrap()
                                                .unwrap()
                                                .value()
                                                .clone();
                                            shelf_e.0.dirs.retain(|id| *id != dir.id);
                                            shelf_table.insert(shelf.id, shelf_e).unwrap();

                                            dirs.remove(&dir.id);
                                            orphan_dirs.insert(dir.clone());
                                            dir_table.remove(dir.id).unwrap();

                                            // currently ignore subdirs, as they should be
                                            // handled in the event / next event
                                        }
                                    }
                                }
                            }
                            EventKind::Other => (), // meta-events
                        }
                    }
                    Ok(Signal::Event(Err(_))) => {}
                    Ok(Signal::Close) => { return }
                    Err(_) => { return }
                }
            }
        });
        Ok(())
    }
}
