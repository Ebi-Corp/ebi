pub mod cache;
pub mod fs;
pub mod network;
pub mod query;
pub mod rpc;
pub mod state;

pub mod prelude {
    pub use crate::services::cache::CacheService;
    pub use crate::services::fs::FileSystem;
    pub use crate::services::network::Network;
    pub use crate::services::query::QueryService;
    pub use crate::services::rpc::RpcService;
    pub use crate::services::state::StateService;
}
