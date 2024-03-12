use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    types, utils,
};

use self::rollup::rollup_keys;

pub mod move_to;
pub mod rebalancing;
pub mod rollup;
pub mod take_from;

fn erase_from_node(target: &mut Node, key: i32) {
    let is_leaf = target.is_leaf;

    if !is_leaf {
        todo!("dead code");
        /*println!("erase_key_data node from={:?} key={}", target.id, key);
        if key < target.keys[0] {
            utils::remove_with_shift(&mut target.data, 0);
            utils::remove_with_shift(&mut target.keys, 0);
            target.keys_count -= 1;
            target.data_count -= 1;
        }
        if key >= target.keys[target.keys_count - 1] {
            utils::remove_with_shift(&mut target.data, target.data_count - 1);
            utils::remove_with_shift(&mut target.keys, target.keys_count - 1);
            target.keys_count -= 1;
            target.data_count -= 1;
        }*/
    } else {
        println!("erase_key_data leaf from={:?} key={}", target.id, key);
    }

    for i in 0..target.keys_count {
        if target.keys[i] == key {
            utils::remove_with_shift(&mut target.keys, i);
            if !target.is_leaf {
                utils::remove_with_shift(&mut target.data, i + 1);
            } else {
                utils::remove_with_shift(&mut target.data, i);
            }
            target.keys_count -= 1;
            target.data_count -= 1;
            break;
        }
    }
}

pub(super) fn erase_key<Storage: NodeStorage>(
    storage: &mut Storage,
    target: &RcNode,
    key: i32,
    root: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    {
        let mut target_ref = target.borrow_mut();
        let first_key = target_ref.keys[0];
        erase_from_node(&mut target_ref, key);
        if target_ref.keys_count > 0 && target_ref.is_leaf && first_key != target_ref.first_key() {
            rollup_keys(
                storage,
                target_ref.parent,
                first_key,
                target_ref.first_key(),
            )?;
        }
        if target_ref.data_count >= storage.get_params().get_min_size_leaf() {
            //update keys in parent
            if first_key != target_ref.keys[0] && target_ref.parent.exists() {
                let parent = storage.get_node(target_ref.parent)?;
                parent
                    .borrow_mut()
                    .update_key(target_ref.id, target_ref.first_key());
            }

            return Ok(root.unwrap());
        }
    }

    return rebalancing::rebalancing(storage, target, root);
}