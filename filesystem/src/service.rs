use std::os::linux::raw;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::dir::{ShelfDir, ShelfDirRef};
use crate::file::{File, FileRef};
use crate::redb::{T_FILE, T_SHELF_DATA, T_SHELF_DIR, T_TAG};
use crate::shelf::ShelfData;
use ebi_proto::rpc::{Data, ReturnCode};
use ebi_types::file::{FileOrder, FileSummary, OrderedFileSummary};
use ebi_types::redb::{Storable, TagStorable};
use ebi_types::shelf::TagRef;
use ebi_types::tag::{Tag, TagData};
use ebi_types::{FileId, ImmutRef, Ref, SharedRef, Uuid, WithPath, get_file_id};
use jwalk::{ClientState, WalkDirGeneric};
use papaya::HashMap;
use papaya::HashSet;
use redb::{Database, Error, ReadableDatabase, ReadableTable, TableDefinition};
use seize::Collector;
use std::sync::Arc;
use std::sync::RwLock;
use tower::Service;

#[derive(Clone)]
pub struct FileSystem {
    pub local_shelves: Arc<HashSet<ImmutRef<ShelfData, FileId>>>,
    pub shelf_dirs: Arc<HashSet<ImmutRef<ShelfDir, FileId>>>,
    pub db: Arc<Database>,
}

pub enum ShelfDirKey {
    Id(FileId),
    Path(PathBuf),
}

struct GetInitShelfDir(ShelfDirKey, PathBuf);

fn setup_tags(raw_tags: HashMap<Uuid, TagStorable>) -> HashSet<SharedRef<Tag>> {
    let tag_refs = Arc::new(HashSet::new());
    let raw_tags = Arc::new(raw_tags);
    fn setup_tag(
        raw_tags: Arc<HashMap<Uuid, TagStorable>>,
        tag_refs: Arc<HashSet<SharedRef<Tag>>>,
        id: Uuid,
        tag_raw: TagStorable,
    ) -> SharedRef<Tag> {
        let parent = if let Some(p_id) = tag_raw.parent {
            match tag_refs.pin().get(&p_id) {
                Some(p) => Some(p.clone()),
                None => {
                    let tag_raw = raw_tags.pin().get(&p_id).unwrap().clone();
                    Some(setup_tag(raw_tags.clone(), tag_refs.clone(), p_id, tag_raw))
                }
            }
        } else {
            None
        };
        let tag = Tag {
            name: tag_raw.name,
            priority: tag_raw.priority,
            parent,
        };
        let s_ref = SharedRef::new_ref_id(id, tag);
        tag_refs.pin().insert(s_ref.clone());
        s_ref
    }
    for (id, tag_raw) in raw_tags.pin().iter() {
        setup_tag(raw_tags.clone(), tag_refs.clone(), *id, tag_raw.clone());
    }
    Arc::into_inner(tag_refs).unwrap()
}

impl FileSystem {
    pub async fn full_load(db_path: &str) -> Result<Self, ReturnCode> {
        let db = Database::create(db_path).map_err(|_| ReturnCode::InternalStateError)?;
        let read_txn = db.begin_read().unwrap();
        let tag_table = read_txn
            .open_table(T_TAG)
            .map_err(|_| ReturnCode::InternalStateError)?;
        let raw_tags = HashMap::new();
        for entry in tag_table
            .range::<Uuid>(..)
            .map_err(|_| ReturnCode::InternalStateError)?
        {
            if let Ok((k, v)) = entry {
                let v = v.value().0; // access TagStorable
                let k = k.value();
                raw_tags.pin().insert(k, v);
            }
        }
        let tag_refs = setup_tags(raw_tags);

        let file_table = read_txn
            .open_table(T_FILE)
            .map_err(|_| ReturnCode::InternalStateError)?;
        let raw_files = HashMap::new();
        for entry in file_table
            .range::<FileId>(..)
            .map_err(|_| ReturnCode::InternalStateError)?
        {
            if let Ok((k, v)) = entry {
                let v = v.value().0;
                let k = k.value();
                raw_files.pin().insert(k, v);
            }
        }
        let tag_refs_pin = tag_refs.pin();
        let files = HashSet::new();
        for (id, file) in raw_files.pin().iter() {
            let tags: crate::dir::HashSet<TagRef> = file
                .tags
                .iter()
                .map(|t_id| tag_refs_pin.get(t_id).unwrap().clone())
                .collect();
            let file = File {
                path: file.path.clone(),
                tags,
            };
            files.pin().insert(ImmutRef::new_ref_id(*id, file));
        }

        let dir_table = read_txn
            .open_table(T_SHELF_DIR)
            .map_err(|_| ReturnCode::InternalStateError)?;
        let raw_dirs = HashMap::new();

        for entry in dir_table
            .range::<FileId>(..)
            .map_err(|_| ReturnCode::InternalStateError)?
        {
            if let Ok((k, v)) = entry {
                let v = v.value().0;
                let k = k.value();
                raw_dirs.pin().insert(k, v);
            }
        }
        let dirs = HashSet::new();

        let all_files = files.pin();
        for (id, dir) in raw_dirs.pin().iter() {
            let collector = Arc::new(Collector::new());
            let files: crate::dir::HashSet<FileRef> = hash_set!(collector);
            let tags = hash_map!(collector);
            let dtags = hash_set!(collector);
            let dtag_dirs = hash_map!(collector);
            let parent = arc_swap::ArcSwap::new(Arc::new(None));
            let subdirs = hash_set!(collector);

            // files
            for f_id in dir.files.iter() {
                let file = all_files.get(f_id).unwrap().clone();
                files.pin().insert(file);
            }

            // tags
            for (t_id, t_files_v) in dir.tags.iter() {
                let t_files = hash_set!(collector);
                let tag = tag_refs.pin().get(t_id).unwrap().clone();
                for f_id in t_files_v {
                    let file = all_files.get(f_id).unwrap().clone();
                    t_files.pin().insert(file);
                }
                tags.pin().insert(tag, t_files);
            }

            // dtags
            for dt_id in dir.dtags.iter() {
                let dtag = tag_refs.pin().get(dt_id).unwrap().clone();
                dtags.pin().insert(dtag);
            }

            // dtag dirs
            for (dt_id, _) in dir.dtag_dirs.iter() {
                let dtag = tag_refs.pin().get(dt_id).unwrap().clone();
                dtag_dirs.pin().insert(dtag, Vec::new());
            }

            let s_dir = ShelfDir {
                path: dir.path.clone(),
                files,
                collector,
                tags,
                dtags,
                dtag_dirs,
                parent,
                subdirs,
            };
            dirs.pin().insert(ImmutRef::new_ref_id(*id, s_dir));
        }
        let raw_dirs = raw_dirs.pin();
        let all_dirs = dirs.pin();
        for dir in all_dirs.iter() {
            let r_dir = raw_dirs.get(&dir.id).unwrap();
            let dtag_dir = dir.dtag_dirs.pin();

            for (d_id, d_dirs) in r_dir.dtag_dirs.iter() {
                let d_dirs: Vec<_> = d_dirs
                    .iter()
                    .map(|dir_id| {
                        let dir_ref = all_dirs.get(dir_id).unwrap();
                        dir_ref.downgrade()
                    })
                    .collect();
                let (dtag_ref, _) = dtag_dir.get_key_value(d_id).unwrap();
                dir.dtag_dirs
                    .pin()
                    .update(dtag_ref.clone(), |_| d_dirs.clone());
            }
            if let Some(p) = r_dir.parent {
                let parent = all_dirs.get(&p).unwrap();
                dir.parent.store(Arc::new(Some(parent.downgrade())));
            };
            for d_id in r_dir.subdirs.iter() {
                let subdir = all_dirs.get(d_id).unwrap();
                dir.subdirs.pin().insert(subdir.downgrade());
            }
        }
        let shelf_table = read_txn
            .open_table(T_SHELF_DATA)
            .map_err(|_| ReturnCode::InternalStateError)?;
        let raw_shelves = HashMap::new();
        for entry in shelf_table
            .range::<FileId>(..)
            .map_err(|_| ReturnCode::InternalStateError)?
        {
            if let Ok((k, v)) = entry {
                let v = v.value().0;
                let k = k.value();
                raw_shelves.pin().insert(k, v);
            }
        }
        let shelves = HashSet::new();
        for (s_id, s_raw) in raw_shelves.pin().iter() {
            let dirs = s_raw
                .dirs
                .iter()
                .map(|s_id| all_dirs.get(s_id).unwrap().downgrade())
                .collect();
            let s_data = ShelfData {
                root: all_dirs.get(&s_raw.root).unwrap().clone(),
                dirs,
                root_path: s_raw.root_path.clone(),
            };
            shelves
                .pin()
                .insert(ImmutRef::<ShelfData, FileId>::new_ref_id(*s_id, s_data));
        }
        drop(all_dirs);
        Ok(FileSystem {
            local_shelves: Arc::new(shelves),
            shelf_dirs: Arc::new(dirs),
            db: Arc::new(db),
        })
    }

    pub async fn get_or_init_dir(
        &mut self,
        shelf: ShelfDirKey,
        subpath: PathBuf,
    ) -> Result<FileId, ReturnCode> {
        self.call(GetInitShelfDir(shelf, subpath)).await
    }

    pub async fn retrieve_dir_recursive(
        &mut self,
        path: PathBuf,
        order: FileOrder,
    ) -> Result<im::HashSet<OrderedFileSummary>, ReturnCode> {
        self.call(RetrieveDirRecursive(path, order)).await
    }

    pub async fn get_or_init_shelf(
        &mut self,
        shelf: ShelfDirKey,
    ) -> Result<ImmutRef<ShelfData, FileId>, ReturnCode> {
        self.call(GetInitShelf(shelf)).await
    }
}

struct RetrieveDirRecursive(PathBuf, FileOrder);

#[derive(Debug, Default, Clone)]
struct DirState {
    dtags: im::HashSet<TagData>,
    files: std::collections::HashSet<OrderedFileSummary>,
}
impl ClientState for DirState {
    type ReadDirState = DirState;
    type DirEntryState = DirState;
}

impl Service<RetrieveDirRecursive> for FileSystem {
    type Response = im::HashSet<OrderedFileSummary>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RetrieveDirRecursive) -> Self::Future {
        let shelf_dirs = self.shelf_dirs.clone();
        Box::pin(async move {
            let root = req.0;
            let order = req.1;
            let files = Arc::new(RwLock::new(im::HashSet::<OrderedFileSummary>::new()));
            let shelf_dirs_c = shelf_dirs.clone();
            let order_c = order.clone();
            WalkDirGeneric::<DirState>::new(root)
                .process_read_dir(move |_depth, path, state, _entries| {
                    if let Ok(dir_id) = get_file_id(path)
                        && let Some(sdir) = shelf_dirs_c.pin().get(&dir_id)
                    {
                        state.dtags = sdir
                            .dtags
                            .pin()
                            .iter()
                            .map(|t| TagData::from(&*t.load_full()))
                            .collect();
                        state.files = sdir
                            .files
                            .pin()
                            .iter()
                            .map(|f| OrderedFileSummary {
                                file_summary: crate::file::gen_summary(
                                    f,
                                    None,
                                    state.dtags.clone(),
                                ),
                                order: order_c.clone(),
                            })
                            .collect();
                    }
                    // [!] further sorting should be implemneted here with _entries.sort() based with
                    // file_order
                })
                .follow_links(false)
                .skip_hidden(true) // must be configurable
                .sort(false) // [TODO] presorting here is probably beneficial with process_read_dir
                .into_iter()
                .for_each(|entry_res| {
                    if let Ok(entry) = entry_res {
                        // [TODO] properly handle errors
                        if entry.file_type().is_file() {
                            if let Ok(metadata) = entry.metadata() {
                                #[cfg(unix)]
                                {
                                    let file_id = FileId::new_inode(metadata.dev(), metadata.ino());
                                    let ordered_file = if let Some(file) =
                                        entry.client_state.files.get(&file_id)
                                    {
                                        file.clone()
                                    } else {
                                        let tags = entry.client_state.dtags.clone();
                                        OrderedFileSummary {
                                            file_summary: FileSummary {
                                                id: file_id,
                                                path: entry.path(),
                                                owner: None,
                                                tags,
                                                metadata: metadata.into(),
                                            },
                                            order: order.clone(),
                                        }
                                    };
                                    files.write().unwrap().insert(ordered_file);
                                }
                                #[cfg(windows)]
                                {
                                    if let Ok(file_id) = get_file_id(entry.path()) {
                                        let ordered_file = if let Some(file) =
                                            entry.client_state.files.get(&file_id)
                                        {
                                            file.clone()
                                        } else {
                                            let tags = entry.client_state.dtags.clone();
                                            OrderedFileSummary {
                                                file_summary: FileSummary {
                                                    id: file_id,
                                                    path: entry.path(),
                                                    owner: None,
                                                    tags,
                                                    metadata: metadata.into(),
                                                },
                                                order: order.clone(),
                                            }
                                        };
                                        files.write().unwrap().insert(ordered_file);
                                    } else {
                                        todo!(); //[!] Handle Error 
                                    }
                                }
                            }
                        }
                    }
                });

            let files = Arc::try_unwrap(files)
                .ok()
                .and_then(|lock| lock.into_inner().ok())
                .unwrap();
            Ok(files)
        })
    }
}

struct GetInitShelf(ShelfDirKey);

impl Service<GetInitShelf> for FileSystem {
    type Response = ImmutRef<ShelfData, FileId>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetInitShelf) -> Self::Future {
        let shelf_dirs = self.shelf_dirs.clone();
        let local_shelves = self.local_shelves.clone();
        let db = self.db.clone();
        Box::pin(async move {
            let shelf_key = req.0;
            let local_shelves = local_shelves.pin();
            let path = match shelf_key {
                ShelfDirKey::Id(id) => {
                    let Some(shelf) = local_shelves.get(&id) else {
                        return Err(ReturnCode::ShelfNotFound);
                    };
                    return Ok(shelf.clone());
                }
                ShelfDirKey::Path(path) => {
                    // might or might not be worth adding a lookup / par iter
                    if let Some(shelf) = local_shelves.iter().find(|s| *s.path() == path) {
                        return Ok(shelf.clone());
                    };
                    if path.is_dir() {
                        path
                    } else if let Some(parent) = path.parent() {
                        parent.to_owned()
                    } else {
                        return Err(ReturnCode::PathNotFound);
                    }
                }
            };
            let mut new_subdir: Option<ImmutRef<ShelfDir, FileId>> = None;
            let mut trav_path = path.clone();
            loop {
                let Ok(file_id) = get_file_id(&trav_path) else {
                    return Err(ReturnCode::InternalStateError);
                };
                let sdir = {
                    if let Some(s_data) = local_shelves.get(&file_id) {
                        return Ok(s_data.clone());
                    } else if let Some(sdir) = shelf_dirs.pin().get(&file_id) {
                        sdir.clone()
                    } else {
                        let Ok(sdir) = ShelfDir::new(path.clone()) else {
                            return Err(ReturnCode::InternalStateError);
                        };
                        let sdir_ref = ImmutRef::<ShelfDir, FileId>::new_ref_id(file_id, sdir);
                        shelf_dirs.pin().insert(sdir_ref.clone());
                        // [TODO] handle db errors properly
                        let write_txn = db
                            .begin_write()
                            .map_err(|_| ReturnCode::InternalStateError)?;
                        let mut table = write_txn
                            .open_table(T_SHELF_DIR)
                            .map_err(|_| ReturnCode::InternalStateError)?;
                        table.insert(file_id, sdir_ref.to_storable());
                        sdir_ref
                    }
                };
                let sdir_wref = sdir.downgrade();

                if let Some(subdir) = new_subdir {
                    subdir.subdirs.pin().insert(sdir_wref.clone());
                }

                new_subdir = Some(sdir.clone());

                if sdir.path == path {
                    let Ok(s_data) = ShelfData::new(sdir) else {
                        return Err(ReturnCode::ShelfCreationIOError);
                    };
                    let s_data_ref: ImmutRef<ShelfData, FileId> = ImmutRef::new_ref(s_data);
                    local_shelves.insert(s_data_ref.clone());
                    return Ok(s_data_ref);
                }
                trav_path = trav_path.parent().unwrap().to_path_buf();
            }
        })
    }
}

impl Service<GetInitShelfDir> for FileSystem {
    type Response = FileId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetInitShelfDir) -> Self::Future {
        let shelf_dirs = self.shelf_dirs.clone();
        let local_shelves = self.local_shelves.clone();
        Box::pin(async move {
            let shelf_key = req.0;
            let r_path = req.1;
            let local_shelves = local_shelves.pin();

            let Some(shelf) = (match shelf_key {
                ShelfDirKey::Id(id) => local_shelves.get(&id),
                ShelfDirKey::Path(path) => local_shelves.iter().find(|s| *s.path() == path),
            }) else {
                return Err(ReturnCode::ShelfNotFound);
            };

            if !r_path.starts_with(&shelf.root_path) {
                return Err(ReturnCode::PathNotFound);
            }
            let path = if !r_path.is_dir() {
                let Some(path) = r_path.parent() else {
                    return Err(ReturnCode::PathNotFound);
                };
                path.to_owned()
            } else {
                r_path
            };
            let mut new_subdir: Option<ImmutRef<ShelfDir, FileId>> = None;
            let mut trav_path = path.clone();
            let Ok(nfile_id) = get_file_id(&trav_path) else {
                return Err(ReturnCode::InternalStateError);
            };
            loop {
                let Ok(file_id) = get_file_id(&trav_path) else {
                    return Err(ReturnCode::InternalStateError);
                };
                let sdir = {
                    if let Some(sdir) = shelf_dirs.pin().get(&file_id) {
                        sdir.clone()
                    } else {
                        let Ok(sdir) = ShelfDir::new(path.clone()) else {
                            return Err(ReturnCode::InternalStateError);
                        };
                        let sdir_ref = ImmutRef::<ShelfDir, FileId>::new_ref_id(file_id, sdir);
                        shelf_dirs.pin().insert(sdir_ref.clone());
                        sdir_ref
                    }
                };

                let sdir_wref = sdir.downgrade();

                if let Some(subdir) = new_subdir {
                    subdir.parent.store(Arc::new(Some(sdir_wref.clone())));
                    sdir.subdirs.pin().insert(subdir.downgrade());
                }

                new_subdir = Some(sdir.clone());

                shelf.dirs.pin().insert(sdir_wref.clone());

                if sdir.path == shelf.root_path {
                    return Ok(nfile_id);
                }

                trav_path = trav_path.parent().unwrap().to_path_buf();
            }
        })
    }
}
