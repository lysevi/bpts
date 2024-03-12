use crate::{map_up_to_tree::map_up, nodestorage::NodeStorage, types, Result};

pub(super) fn rollup_keys<Storage: NodeStorage>(
    storage: &mut Storage,
    id: types::Id,
    key: i32,
    newkey: i32,
) -> Result<()> {
    println!("rollup tree: Id:{:?} key:{} newkey:{}", id, key, newkey);
    let mut f = |x: i32| {
        if x == key {
            println!("update key in {:?}", x);
            return newkey;
        } else {
            return x;
        }
    };
    return map_up(storage, id, &mut f);
}