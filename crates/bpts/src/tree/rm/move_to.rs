use crate::{
    tree::{node::Node, nodestorage::NodeStorage},
    utils::insert_to_array,
    verbose,
};

use super::rollup::rollup_keys;

pub(super) fn move_to_lower(
    target_node: &mut Node,
    low_side_node: &mut Node,
    middle: Option<u32>,
) -> crate::Result<()> {
    verbose!(
        "move_to_lower target={:?} low={:?}",
        target_node.id,
        low_side_node.id
    );
    //if !target_node.is_leaf
    if middle.is_some() {
        insert_to_array(
            &mut low_side_node.keys,
            low_side_node.keys_count,
            middle.unwrap(),
        );
        low_side_node.keys_count += 1;
    }

    let low_keys_count = low_side_node.keys_count;
    for (i, key) in target_node.key_iter().enumerate() {
        low_side_node.keys[low_keys_count + i] = *key;
    }

    let low_data_count = low_side_node.data_count;
    for (num, data) in target_node.data_iter().enumerate() {
        low_side_node.data[low_data_count + num] = data.clone();
    }

    low_side_node.keys_count += target_node.keys_count;
    low_side_node.data_count += target_node.data_count;

    return Ok(());
}

pub(super) fn move_to_higher(
    storage: &mut dyn NodeStorage,
    target: &mut Node,
    high_side: &mut Node,
    middle: Option<u32>,
) {
    verbose!(
        "move_to_higher target={:?} low={:?}",
        target.id,
        high_side.id
    );

    //TODO! opt
    if !target.is_leaf {
        insert_to_array(&mut high_side.keys, 0, middle.unwrap());
        high_side.keys_count += 1;
    }
    for (i, key) in target.key_iter().enumerate() {
        insert_to_array(&mut high_side.keys, i, *key);
    }

    for (i, data) in target.data_iter().enumerate() {
        insert_to_array(&mut high_side.data, i, data.clone());

        if !target.is_leaf {
            let node = storage.get_node(data.into_id()).unwrap();
            node.borrow_mut().parent = high_side.id;
            storage.mark_as_changed(high_side.id);
        }
    }
    storage.mark_as_changed(target.id);
    storage.mark_as_changed(high_side.id);
    high_side.keys_count += target.keys_count;
    high_side.data_count += target.data_count;
}

pub(super) fn try_move_to_low<Storage: NodeStorage>(
    storage: &mut Storage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> crate::Result<bool> {
    if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
        let first_key = target_ref.first_key();
        let mut middle: Option<u32> = None;

        let mut new_min_of_parent: Option<u32> = None;
        if target_ref.parent.exists() {
            let parent = storage.get_node(target_ref.parent)?;
            let mut parent_ref = parent.borrow_mut();
            if !target_ref.is_leaf {
                middle = parent_ref.find_key(first_key, storage.get_cmp());
            }
            new_min_of_parent = Some(parent_ref.first_key());
            parent_ref.erase_link(target_ref.id);
            storage.mark_as_changed(parent_ref.id);
        }

        move_to_lower(target_ref, leaf_ref, middle)?;
        storage.mark_as_changed(leaf_ref.id);

        if !target_ref.is_leaf {
            for i in target_ref.data_iter() {
                let node = storage.get_node(i.into_id())?;
                node.borrow_mut().parent = leaf_ref.id;
                storage.mark_as_changed(node.borrow_mut().id);
            }
        }

        storage.erase_node(&target_ref.id);

        if target_ref.parent.exists() && leaf_ref.parent != target_ref.parent {
            let parent = storage.get_node(target_ref.parent)?;
            if parent.borrow().data_count > 0 {
                let changed_nodes = rollup_keys(
                    storage,
                    target_ref.parent,
                    first_key,
                    new_min_of_parent.unwrap(),
                )?;
                for i in changed_nodes {
                    storage.mark_as_changed(i);
                }
            }
        }

        if target_ref.right.exists() {
            let right_side = storage.get_node(target_ref.right)?;
            right_side.borrow_mut().left = target_ref.left;
            leaf_ref.right = target_ref.right;

            storage.mark_as_changed(right_side.borrow_mut().id);
            storage.mark_as_changed(leaf_ref.id);
        }
        leaf_ref.right = target_ref.right;

        storage.mark_as_changed(target_ref.id);
        storage.mark_as_changed(leaf_ref.id);
        return Ok(true);
    }
    return Ok(false);
}

pub(super) fn try_move_to_high<Storage: NodeStorage>(
    storage: &mut Storage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> crate::Result<bool> {
    if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
        let min_key = leaf_ref.keys[0];
        let mut middle: Option<u32> = None;
        if target_ref.parent.exists() {
            let parent = storage.get_node(leaf_ref.parent)?;
            if !target_ref.is_leaf {
                middle = parent.borrow().find_key(min_key, storage.get_cmp());
            }
            parent.borrow_mut().erase_link(target_ref.id);
        }

        move_to_higher(storage, target_ref, leaf_ref, middle);

        leaf_ref.left = target_ref.left;
        storage.erase_node(&target_ref.id);

        if target_ref.parent.exists() && leaf_ref.parent != target_ref.parent {
            let mut new_min_key = target_ref.first_key();
            if !target_ref.is_leaf {
                let first_data = target_ref.first_data();

                let first_child = storage.get_node(first_data.into_id())?;
                new_min_key = first_child.borrow().first_key();
            }
            //TODO checks;
            rollup_keys(storage, leaf_ref.parent, min_key, new_min_key)?;
        }

        if target_ref.left.exists() {
            let left_side = storage.get_node(target_ref.left)?;
            left_side.borrow_mut().right = target_ref.right;
            leaf_ref.left = target_ref.left;
        }
        storage.mark_as_changed(target_ref.id);
        storage.mark_as_changed(leaf_ref.id);
        return Ok(true);
    }
    return Ok(false);
}
