use crate::shelf::{Shelf, ShelfConfig, ShelfOwner, ShelfType};
use crate::tag::Tag;
use crate::workspace::Workspace;
use crate::{FileId, Uuid};
use bincode::serde::Compat;
use bincode::{decode_from_slice, encode_to_vec};
pub use iroh_base::NodeId;
use redb::{Key, Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

impl Key for Uuid {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl Value for Uuid {
    type SelfType<'a>
        = Uuid
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a uuid::Bytes
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(16)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Uuid(uuid::Uuid::from_slice(data).unwrap())
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("ebi_types::Uuid")
    }
}

impl Key for FileId {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl Value for FileId {
    type SelfType<'a>
        = FileId
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode_from_slice(data, bincode::config::standard())
            .unwrap()
            .0
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("ebi_types::FileId")
    }
}

pub trait Storable: Sized {
    type Storable: Serialize + DeserializeOwned + Debug + 'static;

    fn to_storable(&self) -> Bincode<Self>;
}
#[derive(Debug)]
pub struct Bincode<T: Storable>(pub T::Storable);

impl<T> Clone for Bincode<T>
where
    T: Storable,
    T::Storable: Clone,
{
    fn clone(&self) -> Self {
        Bincode(self.0.clone())
    }
}

impl<T> bincode::Encode for Bincode<T>
where
    T: Storable,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        Compat(&self.0).encode(encoder)?;
        Ok(())
    }
}

impl<T> bincode::Decode<()> for Bincode<T>
where
    T: Storable,
{
    fn decode<D: bincode::de::Decoder<Context = ()>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = Compat::<T::Storable>::decode(decoder)?.0;
        Ok(Bincode(res))
    }
}

impl<T> Value for Bincode<T>
where
    T: Storable + std::fmt::Debug,
{
    type SelfType<'a>
        = Bincode<T>
    where
        Self: 'a;

    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        decode_from_slice(data, bincode::config::standard())
            .unwrap()
            .0
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        encode_to_vec(value, bincode::config::standard()).unwrap()
    }

    fn type_name() -> redb::TypeName {
        use std::any::type_name;

        redb::TypeName::new(&format!("Bincode<{}>", type_name::<T>()))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TagStorable {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Uuid>,
}

impl Storable for Tag {
    type Storable = TagStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(Self::Storable {
            name: self.name.clone(),
            priority: self.priority,
            parent: self.parent.clone().map(|f| f.id),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkspaceStorable {
    pub name: String,
    pub description: String,
    pub shelves: Vec<Uuid>,
    pub tags: Vec<Uuid>,
    pub lookup: HashMap<String, Uuid>,
}
impl<F> Storable for Workspace<F> {
    type Storable = WorkspaceStorable;

    fn to_storable(&self) -> Bincode<Self> {
        let info_data = self.info.load_full();
        Bincode(Self::Storable {
            name: info_data.name.get(),
            description: info_data.description.get(),
            shelves: self.shelves.keys().cloned().collect(),
            tags: self.tags.keys().cloned().collect(),
            lookup: self.lookup.iter().map(|(s, i)| (s.clone(), *i)).collect(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShelfStorable<F> {
    pub name: String,
    pub description: String,
    pub root: PathBuf,
    pub shelf_type: ShelfType,
    pub shelf_owner: ShelfOwner,
    pub config: ShelfConfig,
    pub filter_tags: F,
}

impl<F> Storable for Shelf<F>
where
    for<'a> F: Clone + Serialize + Debug + Deserialize<'a> + 'static,
{
    type Storable = ShelfStorable<F>;

    fn to_storable(&self) -> Bincode<Self> {
        let info_data = self.info.load_full();

        Bincode(Self::Storable {
            name: info_data.name.get(),
            description: info_data.description.get(),
            root: info_data.root.get(),
            shelf_type: self.shelf_type.clone(),
            shelf_owner: self.shelf_owner.clone(),
            config: self.config.clone(),
            filter_tags: self.filter_tags.load_full().as_ref().clone(),
        })
    }
}
