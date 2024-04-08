use crate::{
    tree::{
        node::{KeyCmp, RcNode},
        params::TreeParams,
    },
    types::Id,
};

pub trait NodeStorage {
    fn get_root(&self) -> Option<RcNode>;
    fn get_new_id(&self) -> Id;
    fn get_node(&self, id: Id) -> crate::Result<RcNode>;
    fn add_node(&mut self, node: &RcNode);
    fn erase_node(&mut self, id: &Id);
    fn get_params(&self) -> &TreeParams;
    fn get_cmp(&self) -> &dyn KeyCmp;
}
