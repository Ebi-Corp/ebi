pub mod cache;
pub mod redb;
pub mod service;
use ebi_filesystem::shelf::TagFilter;

pub use ebi_types::tag::Tag;

pub type Workspace = ebi_types::workspace::Workspace<TagFilter>;
pub type Shelf = ebi_types::shelf::Shelf<TagFilter>;
