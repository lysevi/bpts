mod cursor;
pub mod insert;
pub mod map_up_to_tree;
pub mod mocks;
pub mod node;
pub mod nodestorage;
pub mod params;
pub mod prelude;
pub mod read;
pub mod record;
pub mod remove;
pub mod rm;
pub mod split;
pub mod types;
pub mod utils;

pub type Result<T> = std::result::Result<T, types::Error>;
