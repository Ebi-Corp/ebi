macro_rules! hash_set {
    ($collector:expr) => {
        papaya::HashSet::builder()
            .shared_collector($collector.clone())
            .build()
    };
}
macro_rules! hash_map {
    ($collector:expr) => {
        papaya::HashMap::builder()
            .shared_collector($collector.clone())
            .build()
    };
}

pub mod dir;
pub mod file;
pub mod redb;
pub mod service;
pub mod shelf;
pub mod watcher;
use arc_swap::ArcSwap;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug)]
pub struct ModifiablePath(pub(crate) ArcSwap<PathBuf>);

impl ModifiablePath {
    pub fn new(path: PathBuf) -> ModifiablePath {
        ModifiablePath(ArcSwap::new(Arc::new(path)))
    }
    pub fn update(&self, path: &Path) {
        self.0.rcu(|_| path.to_path_buf());
    }
}

impl PartialEq<std::path::PathBuf> for ModifiablePath {
    fn eq(&self, other: &std::path::PathBuf) -> bool {
        *self.0.load_full() == *other
    }
}

impl PartialEq<std::path::Path> for ModifiablePath {
    fn eq(&self, other: &std::path::Path) -> bool {
        *self.0.load_full() == *other
    }
}

impl PartialEq for ModifiablePath {
    fn eq(&self, other: &Self) -> bool {
        *self.0.load_full() == *other.0.load_full()
    }
}

impl Clone for ModifiablePath {
    fn clone(&self) -> Self {
        ModifiablePath::new(self.0.load_full().as_ref().clone())
    }
}
