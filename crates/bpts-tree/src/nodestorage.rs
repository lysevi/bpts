use crate::{
    node::RcNode,
    types::{self, Id},
    Result,
};

pub trait NodeStorage {
    fn get_new_id(&self) -> types::Id;
    //TODO get_node(ptr) -> Option<&Node>;
    fn get_node(&self, id: Id) -> Result<RcNode>;
    //TODO add_node(node) -> ptr
    fn add_node(&mut self, node: &RcNode);

    fn erase_node(&mut self, id: &Id);
}
