use im::HashMap;
use redb::{TableDefinition, ReadTransaction};
use ebi_types::{tag::Tag, FileId, ImmutRef, Uuid};
use ebi_types::redb::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::{dir::ShelfDir, shelf::ShelfData, file::File};
use bincode::serde::Compat;

pub const T_SHELF_DATA: TableDefinition<FileId, ImmutRef<ShelfData>> = TableDefinition::new("shelf_data");
pub const T_SHELF_DIR: TableDefinition<FileId, ImmutRef<ShelfDir>> = TableDefinition::new("shelf_dir");
pub const T_FILE: TableDefinition<FileId, ImmutRef<File>> = TableDefinition::new("file");
pub const T_TAG: TableDefinition<Uuid, ImmutRef<Tag>> = TableDefinition::new("tag");

#[derive(Debug, Deserialize, Serialize)]
pub struct ShelfDataStorable {
    pub root: FileId,
    pub dirs: Vec<FileId>,
    pub root_path: PathBuf
}

impl bincode::Encode for ShelfDataStorable {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        Compat(&self).encode(encoder)?;
        Ok(())
    }
}

impl bincode::Decode<()> for ShelfDataStorable {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = Compat::<ShelfDataStorable>::decode(decoder)?.0;
        Ok(res)
    }
}

impl<'a> Storable<'a> for ShelfData {
    type Storable = ShelfDataStorable;

    fn to_storable(&self) -> Self::Storable {
        ShelfDataStorable {
            root: self.root.id,
            dirs: self.dirs.pin().iter().map(|f| f.id).collect(),
            root_path: self.root_path.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShelfDirStorable {
    pub path: PathBuf,
    pub files: Vec<FileId>,
    pub tags: HashMap<Uuid, Vec<FileId>>, // Tag -> Files
    pub dtags: Vec<Uuid>, // dtags applied from above, to be applied down
    pub dtag_dirs: HashMap<Uuid, Vec<FileId>>, // Dtag -> Dirs
    pub parent: Option<FileId>, // parent dir id
    pub subdirs: Vec<FileId>, // dirs
}

impl bincode::Encode for ShelfDirStorable {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        Compat(&self).encode(encoder)?;
        Ok(())
    }
}

impl bincode::Decode<()> for ShelfDirStorable {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = Compat::<ShelfDirStorable>::decode(decoder)?.0;

        Ok(res)
    }
}

impl<'a> Storable<'a> for ShelfDir {
    type Storable = ShelfDirStorable;

    fn to_storable(&self) -> Self::Storable {
        ShelfDirStorable {
            path: self.path.clone(),
            files: self.files.pin().iter().map(|f| f.id).collect(),
            tags: self.tags.pin().iter().map(|(tag, set)| (tag.id, set.pin().iter().map(|f| f.id).collect()))
            .collect(),
            dtags: self.dtags.pin().iter().map(|t| t.id).collect(),
            dtag_dirs: self.dtag_dirs.pin().iter().map(|(tag, set)| (tag.id, set.iter().map(|f| f.id).collect())).collect(),
            parent: self.parent.load().as_ref().clone().and_then(|s| Some(s.id)),
            subdirs: self.subdirs.pin().iter().map(|d| d.id).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileStorable {
    pub path: PathBuf,
    pub tags: Vec<Uuid>,
}

impl bincode::Encode for FileStorable {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        Compat(&self).encode(encoder)?;
        Ok(())
    }
}

impl bincode::Decode<()> for FileStorable {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = Compat::<FileStorable>::decode(decoder)?.0;

        Ok(res)
    }
}

impl<'a> Storable<'a> for File {
    type Storable = FileStorable;

    fn to_storable(&self) -> Self::Storable {
        FileStorable {
            path: self.path.clone(),
            tags: self.tags.pin().iter().map(|t| t.id).collect(),
        }
    }
}

