use crate::{
    tree::{
        node::{NodeKeyCmp, RcNode},
        TreeParams,
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
    fn get_cmp(&self) -> &dyn NodeKeyCmp;
    fn mark_as_changed(&mut self, id: Id);
}
