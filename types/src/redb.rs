use bincode::{Encode, Decode};
pub use iroh_base::NodeId;
use serde::{Deserialize, Serialize};
use bincode::{decode_from_slice, encode_to_vec};
use redb::{Value, Key};
use std::fmt::Debug;
use crate::{FileId, ImmutRef, Uuid};
use crate::tag::Tag;

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

pub trait Storable<'a>: Sized {
    type Storable: Serialize + Deserialize<'a> + Debug;

    fn to_storable(&self) -> Self::Storable;
}

impl<T, I> Value for ImmutRef<T, I>
where
    T: for<'a> Storable<'a> + std::fmt::Debug,
    for<'a> <T as Storable<'a>>::Storable: Decode<()> + Encode,
    I: std::fmt::Debug
{
    type SelfType<'a>
        = <T as Storable<'a>>::Storable
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

impl<T, I> Key for ImmutRef<T, I>
where
    T: for<'a> Storable<'a> + std::fmt::Debug, 
    for<'a> <T as Storable<'a>>::Storable: Decode<()> + Encode,
    I: std::fmt::Debug

{
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TagStorable {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Uuid>
}

impl bincode::Encode for TagStorable {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        bincode::serde::Compat(&self).encode(encoder)?;
        Ok(())
    }
}

impl bincode::Decode<()> for TagStorable {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = bincode::serde::Compat::<TagStorable>::decode(decoder)?.0;
        Ok(res)
    }
}

#[cfg(feature = "redb")]
impl<'a> Storable<'a> for Tag {
    type Storable = TagStorable;

    fn to_storable(&self) -> Self::Storable {
        TagStorable {
            name: self.name.clone(),
            priority: self.priority,
            parent: self.parent.clone().and_then(|f| Some(f.id))
        }
    }
}
