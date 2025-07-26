use bytes::BufMut;
use enum_dispatch::enum_dispatch;
use paste::paste;
use prost::EncodeError;
pub use prost::Message;
use std::convert::TryFrom;

macro_rules! impl_try_from {
    ($enum_name:ident, $( $variant:ident ),* $(,)?) => {
        impl TryFrom<u8> for $enum_name {
            type Error = ();

            fn try_from(v: u8) -> Result<Self, Self::Error> {
                match v {
                    $(
                        x if x == $enum_name::$variant as u8 => Ok($enum_name::$variant),
                    )*
                    _ => Err(()),
                }
            }
        }
    };
}
macro_rules! impl_res_metadata {
    ($($variant:ident),* $(,)?) => {

        paste! {
            $(
                impl ResMetadata for $variant {
                    fn metadata(&self) -> Option<ResponseMetadata> {
                        self.metadata.clone()
                    }
                }
            )*
        }
    };
}
macro_rules! impl_req_metadata {
    ($($variant:ident),* $(,)?) => {

        $(
            impl ReqMetadata for $variant {
                fn metadata(&self) -> Option<RequestMetadata> {
                    self.metadata.clone()
                }
            }

            impl Encode for $variant {
                fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError> {
                    Message::encode(self, buf)
                }
            }
        )*
    };
}

include!(concat!(env!("OUT_DIR"), "/ebi.rpc.rs"));

#[derive(Debug, PartialEq, Eq)]
pub enum ReturnCode {
    Success = 0,
    PeerNotFound = 1,
    TagNotFound = 2,
    WorkspaceNotFound = 3,
    ShelfNotFound = 4,
    PathNotFound = 5,
    FileNotFound = 6,
    InternalStateError = 7,
    ParseError = 8,
    PeerServiceError = 10,
    DuplicateTag = 201,
    ParentNotFound = 202,
    TagAlreadyAttached = 203,
    NotTagged = 204,
    TagNameEmpty = 205,
    TagNameDuplicate = 206,
    WorkspaceNameEmpty = 304,
    ShelfCreationIOError = 501,
    PathNotDir = 502,
}

pub fn parse_code(code: u32) -> ReturnCode {
    match code {
        0 => ReturnCode::Success,
        1 => ReturnCode::PeerNotFound,
        2 => ReturnCode::TagNotFound,
        3 => ReturnCode::WorkspaceNotFound,
        4 => ReturnCode::ShelfNotFound,
        5 => ReturnCode::PathNotFound,
        6 => ReturnCode::FileNotFound,
        7 => ReturnCode::InternalStateError,
        10 => ReturnCode::PeerServiceError,
        201 => ReturnCode::DuplicateTag,
        202 => ReturnCode::ParentNotFound,
        203 => ReturnCode::TagAlreadyAttached,
        204 => ReturnCode::NotTagged,
        205 => ReturnCode::TagNameEmpty,
        206 => ReturnCode::TagNameDuplicate,
        304 => ReturnCode::WorkspaceNameEmpty,
        501 => ReturnCode::ShelfCreationIOError,
        _ => ReturnCode::ParseError,
    }
}
impl ReturnCode {
    pub fn to_u32(code: ReturnCode) -> u32 {
        match code {
            ReturnCode::ParseError => u32::MAX,
            ReturnCode::Success => 0,
            ReturnCode::PeerNotFound => 1,
            ReturnCode::TagNotFound => 2,
            ReturnCode::WorkspaceNotFound => 3,
            ReturnCode::ShelfNotFound => 4,
            ReturnCode::PathNotFound => 5,
            ReturnCode::FileNotFound => 6,
            ReturnCode::InternalStateError => 7,
            ReturnCode::PeerServiceError => 10,
            ReturnCode::DuplicateTag => 201,
            ReturnCode::ParentNotFound => 202,
            ReturnCode::TagAlreadyAttached => 203,
            ReturnCode::NotTagged => 204,
            ReturnCode::TagNameEmpty => 205,
            ReturnCode::TagNameDuplicate => 206,
            ReturnCode::WorkspaceNameEmpty => 304,
            ReturnCode::ShelfCreationIOError => 501,
            ReturnCode::PathNotDir => 502,
        }
    }
}

#[derive(Debug)]
pub enum MessageType {
    Request = 1,
    Response = 2,
    Data = 3,
    Notification = 4,
    Sync = 5,
}

#[derive(Debug)]
pub enum RequestCode {
    ClientQuery = 1,
    PeerQuery = 2,
    CreateWorkspace = 3,
    EditWorkspace = 4,
    DeleteWorkspace = 5,
    GetWorkspaces = 6,
    AddShelf = 7,
    EditShelf = 8,
    RemoveShelf = 9,
    GetShelves = 10,
    CreateTag = 11,
    EditTag = 12,
    DeleteTag = 13,
    AttachTag = 14,
    DetachTag = 15,
    StripTag = 16,
}

#[enum_dispatch]
pub trait ResMetadata {
    fn metadata(&self) -> Option<ResponseMetadata>;
}

#[enum_dispatch]
pub trait ReqMetadata {
    fn metadata(&self) -> Option<RequestMetadata>;
}

#[enum_dispatch]
pub trait ReqCode {
    fn request_code(&self) -> RequestCode;
}

#[enum_dispatch]
pub trait Encode {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), EncodeError>;
}

//[TODO] Create using a Procedural Macro
#[derive(Clone)]
#[enum_dispatch(ResMetadata)]
pub enum Response {
    CreateTagResponse(CreateTagResponse),
    EditWorkspaceResponse(EditWorkspaceResponse),
    CreateWorkspaceResponse(CreateWorkspaceResponse),
    AttachTagResponse(AttachTagResponse),
    DeleteWorkspaceResponse(DeleteWorkspaceResponse),
    GetWorkspacesResponse(GetWorkspacesResponse),
    EditShelfResponse(EditShelfResponse),
    AddShelfResponse(AddShelfResponse),
    GetShelvesResponse(GetShelvesResponse),
    RemoveShelfResponse(RemoveShelfResponse),
    EditTagResponse(EditTagResponse),
    DeleteTagResponse(DeleteTagResponse),
    DetachTagResponse(DetachTagResponse),
    StripTagResponse(StripTagResponse),
}

impl ReqCode for Response {
    fn request_code(&self) -> RequestCode {
        match self {
            Response::CreateTagResponse(_) => RequestCode::CreateTag,
            Response::EditWorkspaceResponse(_) => RequestCode::EditWorkspace,
            Response::CreateWorkspaceResponse(_) => RequestCode::CreateWorkspace,
            Response::AttachTagResponse(_) => RequestCode::AttachTag,
            Response::DeleteWorkspaceResponse(_) => RequestCode::DeleteWorkspace,
            Response::GetWorkspacesResponse(_) => RequestCode::GetWorkspaces,
            Response::EditShelfResponse(_) => RequestCode::EditShelf,
            Response::AddShelfResponse(_) => RequestCode::AddShelf,
            Response::GetShelvesResponse(_) => RequestCode::GetShelves,
            Response::RemoveShelfResponse(_) => RequestCode::RemoveShelf,
            Response::EditTagResponse(_) => RequestCode::EditTag,
            Response::DeleteTagResponse(_) => RequestCode::DeleteTag,
            Response::DetachTagResponse(_) => RequestCode::DetachTag,
            Response::StripTagResponse(_) => RequestCode::StripTag,
        }
    }
}

impl ReqCode for Request {
    fn request_code(&self) -> RequestCode {
        match self {
            Request::CreateTag(_) => RequestCode::CreateTag,
            Request::EditWorkspace(_) => RequestCode::EditWorkspace,
            Request::CreateWorkspace(_) => RequestCode::CreateWorkspace,
            Request::AttachTag(_) => RequestCode::AttachTag,
            Request::DeleteWorkspace(_) => RequestCode::DeleteWorkspace,
            Request::GetWorkspaces(_) => RequestCode::GetWorkspaces,
            Request::EditShelf(_) => RequestCode::EditShelf,
            Request::AddShelf(_) => RequestCode::AddShelf,
            Request::GetShelves(_) => RequestCode::GetShelves,
            Request::RemoveShelf(_) => RequestCode::RemoveShelf,
            Request::EditTag(_) => RequestCode::EditTag,
            Request::DeleteTag(_) => RequestCode::DeleteTag,
            Request::DetachTag(_) => RequestCode::DetachTag,
            Request::StripTag(_) => RequestCode::StripTag,
            Request::ClientQuery(_) => RequestCode::ClientQuery,
            Request::PeerQuery(_) => RequestCode::PeerQuery,
        }
    }
}

//[TODO] Create using a Procedural Macro
#[derive(Clone)]
#[enum_dispatch(ReqMetadata, Encode)]
pub enum Request {
    CreateTag(CreateTag),
    EditWorkspace(EditWorkspace),
    CreateWorkspace(CreateWorkspace),
    AttachTag(AttachTag),
    DeleteWorkspace(DeleteWorkspace),
    GetWorkspaces(GetWorkspaces),
    EditShelf(EditShelf),
    AddShelf(AddShelf),
    GetShelves(GetShelves),
    RemoveShelf(RemoveShelf),
    EditTag(EditTag),
    DeleteTag(DeleteTag),
    DetachTag(DetachTag),
    StripTag(StripTag),
    ClientQuery(ClientQuery),
    PeerQuery(PeerQuery),
}

impl_res_metadata!(
    CreateTagResponse,
    EditWorkspaceResponse,
    CreateWorkspaceResponse,
    AttachTagResponse,
    DeleteWorkspaceResponse,
    GetWorkspacesResponse,
    EditShelfResponse,
    AddShelfResponse,
    GetShelvesResponse,
    RemoveShelfResponse,
    EditTagResponse,
    DeleteTagResponse,
    DetachTagResponse,
    StripTagResponse,
    QueryResponse,
);

impl_req_metadata!(
    CreateTag,
    EditWorkspace,
    CreateWorkspace,
    AttachTag,
    DeleteWorkspace,
    GetWorkspaces,
    EditShelf,
    AddShelf,
    GetShelves,
    RemoveShelf,
    EditTag,
    DeleteTag,
    DetachTag,
    StripTag,
    PeerQuery,
    ClientQuery
);

#[derive(Debug)]
pub enum DataCode {
    ClientQueryData = 1,
    PeerQueryData = 2,
}

#[derive(Debug)]
pub enum NotificationCode {
    Heartbeat = 1,
    Operation = 2,
    PeerConnected = 3,
}

#[derive(Debug)]
pub enum SyncCode {
    Sync = 1,
}
impl_try_from!(MessageType, Request, Response, Data, Notification, Sync);

impl_try_from!(
    RequestCode,
    ClientQuery,
    PeerQuery,
    CreateTag,
    EditWorkspace,
    DeleteWorkspace,
    GetWorkspaces,
    AddShelf,
    EditShelf,
    RemoveShelf,
    GetShelves,
    CreateTag,
    EditTag,
    DeleteTag,
    AttachTag,
    DetachTag,
    StripTag
);

impl_try_from!(DataCode, ClientQueryData, PeerQueryData);

impl_try_from!(NotificationCode, Heartbeat, Operation, PeerConnected);

impl_try_from!(ActionTarget, Workspace, Shelf, Tag);

impl_try_from!(ActionType, Create, Edit, Delete);

impl_try_from!(SyncCode, Sync);
