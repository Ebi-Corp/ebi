pub mod file;
pub mod sharedref;
pub mod shelf;
pub mod stateful;
pub mod tag;
pub mod workspace;

use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

pub use crate::sharedref::*;
pub use crate::stateful::*;
pub use iroh_base::NodeId;
use serde::Deserialize;
use serde::Serialize;
pub use uuid::Uuid as RawUuid;
pub type RequestId = Uuid;
pub use file_id::FileId;

#[cfg(feature = "redb")]
use redb::Key;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Uuid(RawUuid);

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
        self.0.fmt(f)
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

impl<'de> Deserialize<'de> for Uuid{
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

#[cfg(feature = "redb")]
impl Key for Uuid {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}
