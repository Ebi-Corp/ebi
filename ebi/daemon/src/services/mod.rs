pub mod cache;
pub mod filesys;
pub mod peer;
pub mod query;
pub mod rpc;
pub mod state;

pub mod prelude {
    pub use crate::services::cache::CacheService;
    pub use crate::services::filesys::FileSysService;
    pub use crate::services::peer::PeerService;
    pub use crate::services::query::QueryService;
    pub use crate::services::rpc::RpcService;
    pub use crate::services::state::StateService;
}
