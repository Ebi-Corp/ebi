use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::NodeId;
use crate::Ref;
use crate::Uuid;
use crate::tag::Tag;
use crate::{InfoState, SharedRef, StatefulField, StatefulRef};
use arc_swap::ArcSwap;
use chrono::Duration;
use serde::{Deserialize, Serialize};

pub type ShelfId = Uuid;
pub type TagRef = SharedRef<Tag>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum ShelfType {
    Local,
    Remote,
}

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub enum ShelfOwner {
    Node(NodeId),
    Sync(SyncId),
}

//[#] Sync

pub type SyncId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncConfig {
    //[!] Placeholder for sync configuration
    pub interval: Option<Duration>, // Auto-Sync Interval
    pub auto_sync: bool,            // Auto-Sync on changes
}

//[#] Sync

#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ShelfConfig {
    //[TODO] Define a configuration for the shelf
    pub sync_config: Option<SyncConfig>,
}

#[derive(Debug)]
pub struct Shelf<TagFilter> {
    pub shelf_type: ShelfType,
    pub shelf_owner: ShelfOwner,
    pub config: ShelfConfig,
    pub filter_tags: ArcSwap<TagFilter>,
    pub info: StatefulRef<ShelfInfo>,
}

impl<TagFilter> Clone for Shelf<TagFilter> {
    fn clone(&self) -> Self {
        Shelf {
            shelf_type: self.shelf_type.clone(),
            shelf_owner: self.shelf_owner.clone(),
            config: self.config.clone(),
            filter_tags: ArcSwap::new(self.filter_tags.load_full()),
            info: self.info.clone_inner(),
        }
    }
}

impl<TagFilter: Default> Shelf<TagFilter> {
    pub fn new(
        path: PathBuf,
        name: String,
        shelf_type: ShelfType,
        shelf_owner: ShelfOwner,
        config: Option<ShelfConfig>,
        description: String,
    ) -> Self {
        let shelf = Shelf {
            shelf_type,
            shelf_owner,
            config: config.unwrap_or_default(),
            filter_tags: ArcSwap::new(Arc::new(TagFilter::default())), // [TODO] Filter parameters (size, ...) should be configurable
            info: StatefulRef::new_ref(ShelfInfo::new(Some(name), Some(description), path)),
        };
        shelf
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum ShelfInfoField {
    Name,
    Description,
    Root,
}

#[derive(Debug)]
pub struct ShelfInfo {
    pub name: StatefulField<ShelfInfoField, String>,
    pub description: StatefulField<ShelfInfoField, String>,
    pub root: StatefulField<ShelfInfoField, PathBuf>,
}

impl ShelfInfo {
    pub fn new(name: Option<String>, description: Option<String>, root_path: PathBuf) -> Self {
        let default_name = root_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("Unnamed"))
            .to_string_lossy()
            .to_string();
        let default_description = "".to_string();
        let name = name.unwrap_or(default_name);
        let description = description.unwrap_or(default_description);
        let info_state: InfoState<ShelfInfoField> = InfoState::new();
        ShelfInfo {
            name: {
                let field = StatefulField::<ShelfInfoField, String>::new(
                    ShelfInfoField::Name,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&name);
                drop(updater); // No State Update required for Info Creation
                field
            },
            description: {
                let field = StatefulField::<ShelfInfoField, String>::new(
                    ShelfInfoField::Description,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&description);
                drop(updater); // No State Update required for Info Creation
                field
            },
            root: {
                let field = StatefulField::<ShelfInfoField, PathBuf>::new(
                    ShelfInfoField::Root,
                    info_state.clone(),
                );
                let (field, updater) = field.set(&root_path);
                drop(updater); // No State Update required for Info Creation
                field
            },
        }
    }
}
