pub(self) mod cmp;
pub mod flat_storage;
pub mod node_storage;
pub mod store;

use std::{cell::RefCell, rc::Rc};

use crate::tree::{node::NodeKeyCmp, TreeParams};

pub trait KeyCmp {
    fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering;
}

pub type KeyCmpRc = Rc<RefCell<dyn NodeKeyCmp>>;

#[derive(Clone, Copy)]
pub struct StorageParams {
    offset: u32,
    is_closed: u8,
    pub tree_params: TreeParams,
}

impl StorageParams {
    pub fn default() -> Self {
        Self {
            offset: 0,
            is_closed: 1,
            tree_params: TreeParams::default(),
        }
    }
}
