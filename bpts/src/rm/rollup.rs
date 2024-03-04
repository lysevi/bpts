use crate::{nodestorage::NodeStorage, types};

pub(super) fn rollup_keys(
    storage: &mut dyn NodeStorage,
    id: types::Id,
    key: i32,
    newkey: i32,
) -> Result<(), types::Error> {
    println!("rollup tree: Id:{:?} key:{} newkey:{}", id, key, newkey);
    let mut id_of_parent = id;
    while id_of_parent.exists() {
        let node = storage.get_node(id_of_parent)?;
        let mut refn = node.borrow_mut();

        for i in 0..refn.keys_count {
            if refn.keys[i] == key {
                println!("update key in {:?}", refn.id);
                refn.keys[i] = newkey;
                break;
            }
        }

        id_of_parent = refn.parent;
    }
    return Ok(());
}
