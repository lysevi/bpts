use crate::{map_up_to_tree::map_up, nodestorage::NodeStorage, types, Result};

pub(super) fn rollup_keys<Storage: NodeStorage>(
    storage: &Storage,
    id: types::Id,
    key: u32,
    newkey: u32,
) -> Result<()> {
    println!("rollup tree: Id:{:?} key:{} newkey:{}", id, key, newkey);
    let cmp = storage.get_cmp();
    let mut f = |x: u32| {
        if cmp.compare(x, key).is_eq() {
            println!("update key in {:?}", x);
            return newkey;
        } else {
            return x;
        }
    };
    return map_up(storage, id, &mut f);
}
