use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::dir::ShelfDir;
use crate::shelf::ShelfData;
use ebi_proto::rpc::ReturnCode;
use ebi_types::file::{FileOrder, FileSummary, OrderedFileSummary};
use ebi_types::tag::TagData;
use ebi_types::{ImmutRef, Ref, WithPath};
use file_id::{FileId, get_file_id};
use jwalk::{ClientState, WalkDirGeneric};
use papaya::HashSet;
use std::sync::Arc;
use std::sync::RwLock;
use tower::Service;

#[derive(Clone)]
pub struct FileSystem {
    pub local_shelves: Arc<HashSet<ImmutRef<ShelfData, FileId>>>,
    pub shelf_dirs: Arc<HashSet<ImmutRef<ShelfDir, FileId>>>,
}

pub enum ShelfDirKey {
    Id(FileId),
    Path(PathBuf),
}

struct GetInitShelfDir(ShelfDirKey, PathBuf);

impl FileSystem {
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
                                file_summary: crate::file::gen_summary(f, None),
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
                                let file_id = FileId::new_inode(metadata.dev(), metadata.ino());
                                let ordered_file =
                                    if let Some(file) = entry.client_state.files.get(&file_id) {
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
