use crate::{
    types::{self, Id},
    Result,
};

use super::nodestorage::NodeStorage;

pub fn map_up<Action, Storage: NodeStorage>(
    storage: &Storage,
    id: types::Id,
    act: &mut Action,
) -> Result<Vec<Id>>
where
    Action: FnMut(u32) -> u32,
{
    let mut id_of_parent = id;
    let mut result = Vec::new();
    while id_of_parent.exists() {
        let node = storage.get_node(id_of_parent)?;
        let mut refn = node.borrow_mut();
        //TODO return slice of changed nodes and call 'mark_as_changed' in call code
        result.push(refn.id);
        for i in 0..refn.keys_count {
            refn.keys[i] = act(refn.keys[i]);
        }

        id_of_parent = refn.parent;
    }
    return Ok(result);
}
