use crate::{
    node::RcNode,
    types::{self, Id},
};

pub trait NodeStorage {
    fn get_new_id(&self) -> i32;
    //TODO get_node(ptr) -> Option<&Node>;
    fn get_node(&self, id: Id) -> Result<RcNode, types::Error>;
    //TODO add_node(node) -> ptr
    fn add_node(&mut self, node: &RcNode);
}
