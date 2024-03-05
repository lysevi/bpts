pub mod insert;
pub mod map_up_to_tree;
pub mod mfile;
pub mod mocks;
pub mod node;
pub mod nodestorage;
pub mod read;
pub mod rec;
pub mod remove;
pub mod rm;
pub mod split;
pub mod types;
pub mod utils;

pub use insert::insert;
pub use mocks::MockNodeStorage;
pub use node::Node;
pub use nodestorage::NodeStorage;
pub use read::find;
pub use rec::Record;
pub use types::Id;

pub type Result<T> = std::result::Result<T, types::Error>;
