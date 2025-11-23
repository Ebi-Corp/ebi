use bincode::serde::Compat;
pub use iroh_base::NodeId;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
pub use uuid::Uuid as RawUuid;
pub type RequestId = Uuid;
pub use file_id::FileId as RawFileId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Uuid(pub RawUuid);

impl Uuid {
    // re-export UUID namespaces
    pub const NAMESPACE_DNS: Self = Uuid(RawUuid::NAMESPACE_DNS);
    pub const NAMESPACE_OID: Self = Uuid(RawUuid::NAMESPACE_OID);
    pub const NAMESPACE_URL: Self = Uuid(RawUuid::NAMESPACE_URL);

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Uuid(RawUuid::from_bytes(bytes))
    }

    pub fn from(uuid: RawUuid) -> Self {
        Uuid(uuid)
    }

    pub fn into(self) -> RawUuid {
        self.0
    }

    pub fn new_v4() -> Self {
        Uuid(RawUuid::new_v4())
    }
    pub fn new_v5(namespace: &Uuid, name: &[u8]) -> Uuid {
        Uuid(RawUuid::new_v5(&namespace, name))
    }
    pub fn now_v7() -> Self {
        Uuid(RawUuid::now_v7())
    }
}

impl FromStr for Uuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Uuid(RawUuid::from_str(s)?))
    }
}
impl Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<Uuid> for std::vec::Vec<u8> {
    fn from(value: Uuid) -> Vec<u8> {
        value.0.into()
    }
}
impl TryFrom<Vec<u8>> for Uuid {
    type Error = uuid::Error;
    fn try_from(value: std::vec::Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Uuid(RawUuid::from_slice(&value)?))
    }
}

impl Deref for Uuid {
    type Target = RawUuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Serialize for Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        RawUuid::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw_uuid = RawUuid::deserialize(deserializer)?;
        Ok(Uuid(raw_uuid))
    }
}

pub fn parse_peer_id(bytes: &[u8]) -> Result<NodeId, ()> {
    let bytes: &[u8; 32] = bytes.try_into().map_err(|_| ())?;
    NodeId::from_bytes(bytes).map_err(|_| ())
}

#[derive(Debug)]
pub enum UuidErr {
    SizeMismatch,
}

pub fn uuid(bytes: &[u8]) -> Result<Uuid, UuidErr> {
    let bytes: Result<[u8; 16], _> = bytes.to_owned().try_into();
    if let Ok(bytes) = bytes {
        Ok(Uuid::from_bytes(bytes))
    } else {
        Err(UuidErr::SizeMismatch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct FileId(pub RawFileId);

impl bincode::Decode<()> for FileId {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let res = Compat::<FileId>::decode(decoder)?.0;
        Ok(res)
    }
}

impl bincode::Encode for FileId {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        Compat(&self).encode(encoder)?;
        Ok(())
    }
}

impl Deref for FileId {
    type Target = RawFileId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for FileId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        RawFileId::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for FileId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw_id = RawFileId::deserialize(deserializer)?;
        Ok(FileId(raw_id))
    }
}

pub fn get_file_id(path: impl AsRef<std::path::Path>) -> std::io::Result<FileId> {
    Ok(FileId(file_id::get_file_id(path)?))
}
impl FileId {
    pub fn new_inode(device_id: u64, inode_number: u64) -> Self {
        FileId(file_id::FileId::Inode {
            device_id,
            inode_number,
        })
    }

    pub fn new_low_res(volume_serial_number: u32, file_index: u64) -> Self {
        FileId(file_id::FileId::LowRes {
            volume_serial_number,
            file_index,
        })
    }

    pub fn new_high_res(volume_serial_number: u64, file_id: u128) -> Self {
        FileId(file_id::FileId::HighRes {
            volume_serial_number,
            file_id,
        })
    }
}
