use crate::{node::Node, nodestorage::NodeStorage, utils, Result};

use super::rollup::rollup_keys;

pub(super) fn move_to_lower(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    low_side_node: &mut Node,
    middle: Option<i32>,
) -> Result<()> {
    println!(
        "move_to_lower target={:?} low={:?}",
        target_node.id, low_side_node.id
    );
    //if !target_node.is_leaf
    if middle.is_some() {
        utils::insert_to_array(
            &mut low_side_node.keys,
            low_side_node.keys_count,
            middle.unwrap(),
        );
        low_side_node.keys_count += 1;
    }

    let low_keys_count = low_side_node.keys_count;
    for i in 0..target_node.keys_count {
        low_side_node.keys[low_keys_count + i] = target_node.keys[i];
    }

    let low_data_count = low_side_node.data_count;
    for i in 0..target_node.data_count {
        let node_ptr = target_node.data[i].clone();
        low_side_node.data[low_data_count + i] = node_ptr.clone();

        if !target_node.is_leaf {
            //TODO! move to resize
            let node = storage.get_node(node_ptr.into_id())?;
            node.borrow_mut().parent = low_side_node.id;
        }
    }

    low_side_node.keys_count += target_node.keys_count;
    low_side_node.data_count += target_node.data_count;

    return Ok(());
}

pub(super) fn move_to_higher(
    storage: &mut dyn NodeStorage,
    target: &mut Node,
    high_side: &mut Node,
    middle: Option<i32>,
) {
    println!(
        "move_to_higher target={:?} low={:?}",
        target.id, high_side.id
    );

    //TODO! opt
    if !target.is_leaf {
        utils::insert_to_array(&mut high_side.keys, 0, middle.unwrap());
        high_side.keys_count += 1;
    }
    for i in 0..target.keys_count {
        utils::insert_to_array(&mut high_side.keys, i, target.keys[i]);
    }

    for i in 0..target.data_count {
        utils::insert_to_array(&mut high_side.data, i, target.data[i].clone());

        if !target.is_leaf {
            let node = storage.get_node(target.data[i].into_id()).unwrap();
            node.borrow_mut().parent = high_side.id;
        }
    }

    high_side.keys_count += target.keys_count;
    high_side.data_count += target.data_count;
}

pub(super) fn try_move_to_low(
    storage: &mut dyn NodeStorage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> Result<bool> {
    if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
        let min_key = target_ref.keys[0];
        let mut middle: Option<i32> = None;

        let mut new_min_of_parent: Option<i32> = None;
        if target_ref.parent.exists() {
            let parent = storage.get_node(target_ref.parent)?;
            if !target_ref.is_leaf {
                middle = parent.borrow().find_key(min_key);
            }
            new_min_of_parent = Some(parent.borrow().first_key());
            parent.borrow_mut().erase_link(target_ref.id);
        }
        let first_key = target_ref.first_key();

        move_to_lower(storage, target_ref, leaf_ref, middle)?;
        storage.erase_node(&target_ref.id);

        if target_ref.parent.exists() {
            if leaf_ref.parent != target_ref.parent {
                let parent = storage.get_node(target_ref.parent)?;
                if parent.borrow().data_count > 0 {
                    rollup_keys(
                        storage,
                        target_ref.parent,
                        first_key,
                        new_min_of_parent.unwrap(),
                    )?;
                }
            }
        }

        //TODO! check result;
        if target_ref.right.exists() {
            let right_side = storage.get_node(target_ref.right)?;
            right_side.borrow_mut().left = target_ref.left;
            leaf_ref.right = target_ref.right;
        }
        leaf_ref.right = target_ref.right;
        return Ok(true);
    }
    return Ok(false);
}

pub(super) fn try_move_to_high(
    storage: &mut dyn NodeStorage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> Result<bool> {
    if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
        let min_key = leaf_ref.keys[0];
        let mut middle: Option<i32> = None;
        if target_ref.parent.exists() {
            let parent = storage.get_node(leaf_ref.parent)?;
            if !target_ref.is_leaf {
                middle = parent.borrow().find_key(min_key);
            }
            parent.borrow_mut().erase_link(target_ref.id);
        }

        let old_min_key = leaf_ref.first_key();
        move_to_higher(storage, target_ref, leaf_ref, middle);

        leaf_ref.left = target_ref.left;
        storage.erase_node(&target_ref.id);

        if target_ref.parent.exists() {
            if leaf_ref.parent != target_ref.parent {
                let mut new_min_key = target_ref.first_key();
                if !target_ref.is_leaf {
                    let first_data = target_ref.first_data();

                    let first_child = storage.get_node(first_data.into_id())?;
                    new_min_key = first_child.borrow().first_key();
                }
                //TODO checks;
                rollup_keys(storage, leaf_ref.parent, old_min_key, new_min_key)?;
            }
        }

        if target_ref.left.exists() {
            let left_side = storage.get_node(target_ref.left)?;
            left_side.borrow_mut().right = target_ref.right;
            leaf_ref.left = target_ref.left;
        }

        return Ok(true);
    }
    return Ok(false);
}
