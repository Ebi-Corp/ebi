use crate::tag::Tag;
use crate::{FileId, Uuid};
use bincode::serde::Compat;
use bincode::{Decode, Encode};
use bincode::{decode_from_slice, encode_to_vec};
pub use iroh_base::NodeId;
use redb::{Key, Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
        Some(16)
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
        redb::TypeName::new("ebi_types::Uuid")
    }
}

pub trait Storable: Sized {
    type Storable: Serialize + DeserializeOwned + Debug + 'static;

    fn to_storable(&self) -> Bincode<Self>;
}
#[derive(Debug)]
pub struct Bincode<T: Storable>(pub T::Storable);

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
            parent: self.parent.clone().and_then(|f| Some(f.id)),
        })
    }
}
