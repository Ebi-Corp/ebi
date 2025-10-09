use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::prelude::*;
use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::shelf::ShelfDataRef;
use crate::shelf::dir::ShelfDir;
use crate::shelf::file::FileSummary;
use crate::tag::TagData;
use ebi_proto::rpc::ReturnCode;
use file_id::{FileId, get_file_id};
use jwalk::{ClientState, WalkDirGeneric};
use papaya::HashSet;
use tower::Service;

#[derive(Clone)]
pub struct FileSysService {
    pub shelf_dirs: Arc<HashSet<ImmutRef<ShelfDir, FileId>>>,
}
struct ShelfDirKey {
    id: FileId,
    path: PathBuf,
}

impl PartialEq for ShelfDirKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

struct GetInitShelfDir(ShelfDataRef, PathBuf);

impl FileSysService {
    pub async fn get_or_init_dir(
        &mut self,
        shelf: ShelfDataRef,
        path: PathBuf,
    ) -> Result<FileId, ReturnCode> {
        self.call(GetInitShelfDir(shelf, path)).await
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
        path: PathBuf,
    ) -> Result<ImmutRef<ShelfDir, FileId>, ReturnCode> {
        self.call(GetInitShelf(path)).await
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

impl Service<RetrieveDirRecursive> for FileSysService {
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
            let files = Arc::new(HashSet::<OrderedFileSummary>::new());
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
                                file_summary: FileSummary::from(f, None),
                                order: order_c.clone(),
                            })
                            .collect();
                    }
                    // [!] further sorting should be implemneted here with _entries.sort() based with
                    // file_order
                })
                .follow_links(false)
                .skip_hidden(false)
                .sort(false) // [TODO] presorting here is probably beneficial with process_read_dir
                .into_iter()
                .for_each(|entry_res| {
                    let entry = entry_res.unwrap(); // [TODO] properly handle errors
                    if entry.file_type().is_file() {
                        if let Ok(file_id) = get_file_id(entry.path()) {
                            let ordered_file =
                                if let Some(file) = entry.client_state.files.get(&file_id) {
                                    file.clone()
                                } else {
                                    let tags = entry.client_state.dtags.clone();
                                    OrderedFileSummary {
                                        file_summary: FileSummary::new(
                                            file_id,
                                            entry.path(),
                                            None,
                                            tags,
                                        ),
                                        order: order.clone(),
                                    }
                                };
                            files.pin().insert(ordered_file);
                        }
                    }
                });
            Ok(files.pin().iter().cloned().collect())
        })
    }
}

struct GetInitShelf(PathBuf);

impl Service<GetInitShelf> for FileSysService {
    type Response = ImmutRef<ShelfDir, FileId>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetInitShelf) -> Self::Future {
        let shelf_dirs = self.shelf_dirs.clone();
        Box::pin(async move {
            let r_path = req.0;
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
                    subdir.subdirs.pin().insert(sdir_wref.clone());
                }

                new_subdir = Some(sdir.clone());

                if sdir.path == path {
                    break Ok(sdir);
                }
                trav_path = trav_path.parent().unwrap().to_path_buf();
            }
        })
    }
}

impl Service<GetInitShelfDir> for FileSysService {
    type Response = FileId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetInitShelfDir) -> Self::Future {
        let shelf_dirs = self.shelf_dirs.clone();
        Box::pin(async move {
            let shelf = req.0;
            let r_path = req.1;
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

                // if the dir already existed in the shelf, we are done
                if !shelf.dirs.pin().insert(sdir_wref.clone()) {
                    break Ok(nfile_id);
                }
                trav_path = trav_path.parent().unwrap().to_path_buf();
            }
        })
    }
}
