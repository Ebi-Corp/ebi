pub mod sharedref;
pub mod stateful;
pub mod tag;
pub mod workspace;
pub mod shelf;
pub mod file;

pub use crate::sharedref::*;
pub use crate::stateful::*;
pub use uuid::Uuid;
pub use iroh_base::NodeId;
pub type RequestId = Uuid;
pub use file_id::FileId;

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
