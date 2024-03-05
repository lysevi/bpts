pub mod insert;
mod map_up_to_tree;
pub mod mfile;
pub mod mocks;
pub mod node;
pub mod nodestorage;
pub mod read;
pub mod rec;
pub mod remove;
mod rm;
pub mod split;
pub mod types;
pub mod utils;

pub type Result<T> = std::result::Result<T, types::Error>;
