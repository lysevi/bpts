use crate::{nodestorage::NodeStorage, types, Result};

pub fn map_up<Action>(storage: &mut dyn NodeStorage, id: types::Id, act: &mut Action) -> Result<()>
where
    Action: FnMut(i32) -> i32,
{
    let mut id_of_parent = id;
    while id_of_parent.exists() {
        let node = storage.get_node(id_of_parent)?;
        let mut refn = node.borrow_mut();

        for i in 0..refn.keys_count {
            refn.keys[i] = act(refn.keys[i]);
        }

        id_of_parent = refn.parent;
    }
    return Ok(());
}
