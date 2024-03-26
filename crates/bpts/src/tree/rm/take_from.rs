use crate::{
    tree::{node::Node, nodestorage::NodeStorage},
    types::Id,
    utils::{insert_to_array, remove_with_shift},
    verbose,
};

use super::rollup::rollup_keys;

pub(super) fn take_from_low<Storage: NodeStorage>(
    storage: &mut Storage,
    target: &mut Node,
    low_side: &mut Node,
    middle: Option<u32>,
) {
    verbose!("take_from_low target={:?} low={:?}", target.id, low_side.id);

    //let mut min_key = target.first_key();

    if !target.is_leaf && middle.is_some() {
        verbose!("take_from_low insert middle");
        insert_to_array(&mut target.keys, 0, middle.unwrap());
        target.keys_count += 1;
    }

    let max_key = low_side.last_key();
    let max_data = low_side.last_data();

    if !target.is_leaf {
        let min_data_node = storage.get_node(max_data.into_id()).unwrap();
        min_data_node.borrow_mut().parent = target.id;
    } else {
        insert_to_array(&mut target.keys, 0, max_key);
        target.keys_count += 1;
    }

    //utils::insert_to_array(&mut target.keys, 0, max_key);
    insert_to_array(&mut target.data, 0, max_data);
    low_side.keys_count -= 1;
    low_side.data_count -= 1;

    //target.keys_count += 1;
    target.data_count += 1;
}

pub(super) fn take_from_high(target: &mut Node, high_side: &mut Node, middle: Option<u32>) -> u32 {
    verbose!(
        "take_key_from_high target={:?} high={:?} minKey={}",
        target.id,
        high_side.id,
        high_side.keys[0]
    );

    let mut min_key = high_side.first_key();
    let mut result = min_key;
    let min_data = high_side.first_data();
    if !target.is_leaf {
        min_key = middle.unwrap();
        verbose!(" new minKey={}", min_key);
    }
    {
        let mut position = target.keys_count;
        target.keys[position] = min_key;
        position = target.data_count;
        target.data[position] = min_data;

        remove_with_shift(&mut high_side.keys, 0);
        remove_with_shift(&mut high_side.data, 0);

        high_side.keys_count -= 1;
        high_side.data_count -= 1;

        target.keys_count += 1;
        target.data_count += 1;
    }
    if target.is_leaf {
        result = high_side.first_key();
    }
    return result;
}

pub(super) fn try_take_from_low<Storage: NodeStorage>(
    storage: &mut Storage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> crate::Result<bool> {
    if !target_ref.is_leaf && target_ref.id == Id(78) {
        verbose!("!");
    }
    if leaf_ref.data_count > t {
        let mut middle: Option<u32> = None;
        let mut first_key = target_ref.first_key();
        let taken_key = leaf_ref.keys[leaf_ref.keys_count - 1];
        if !target_ref.is_leaf {
            if leaf_ref.parent == target_ref.parent {
                let parent = storage.get_node(target_ref.parent)?;
                let parent_ref = parent.borrow_mut();
                middle = parent_ref.find_key(first_key, storage.get_cmp());
            }
            // else if leaf_ref.parent != target_ref.parent {
            //     let parent = storage.get_node(target_ref.parent)?;
            //     let parent_ref = parent.borrow_mut();
            //     middle = parent_ref.find_key(first_key, storage.get_cmp())
            // }
            if middle.is_none() {
                let first_child = storage.get_node(target_ref.data[0].into_id())?;
                middle = Some(first_child.borrow().first_key());
            }
            // } else {
            //     todo!();
            // }
        }
        take_from_low(storage, target_ref, leaf_ref, middle);
        if !target_ref.is_leaf {
            let taked_id = target_ref.first_data().into_id();
            let taked_node = storage.get_node(taked_id)?;
            taked_node.borrow_mut().parent = target_ref.id;
        }
        if target_ref.parent.exists() {
            let link_to_parent = storage.get_node(target_ref.parent)?;
            link_to_parent
                .borrow_mut()
                .update_key(target_ref.id, taken_key);

            let mut new_min_key = taken_key;
            if !target_ref.is_leaf {
                let link_to_first = target_ref.first_data();

                let first_child = storage.get_node(link_to_first.into_id())?;
                new_min_key = first_child.borrow().first_key();

                let second_data = target_ref.data[1].clone();

                let second_child = storage.get_node(second_data.into_id())?;
                first_key = second_child.borrow().first_key();
            }

            if leaf_ref.parent != target_ref.parent {
                rollup_keys(storage, target_ref.parent, first_key, new_min_key)?;
            }
        }
        return Ok(true);
    }
    return Ok(false);
}

pub(super) fn try_take_from_high<Storage: NodeStorage>(
    storage: &mut Storage,
    target_ref: &mut Node,
    leaf_ref: &mut Node,
    t: usize,
) -> crate::Result<bool> {
    if leaf_ref.data_count > t {
        let mut first_key = leaf_ref.keys[0];
        if !leaf_ref.is_leaf {
            let first_child = storage.get_node(leaf_ref.first_data().clone().into_id())?;
            first_key = first_child.borrow().first_key();
        }

        let min_key = leaf_ref.keys[0];
        let mut middle: Option<u32> = None;
        if !target_ref.is_leaf {
            if leaf_ref.parent == target_ref.parent {
                let parent = storage.get_node(leaf_ref.parent)?;
                middle = parent.borrow().find_key(min_key, storage.get_cmp());
            } else {
                let first_high_child = storage.get_node(leaf_ref.first_data().into_id())?;
                middle = Some(first_high_child.borrow().first_key());
            }
        }

        let new_min_key = take_from_high(target_ref, leaf_ref, middle);
        if !target_ref.is_leaf {
            let taked_id = target_ref.last_data().into_id();
            let taked_node = storage.get_node(taked_id)?;
            taked_node.borrow_mut().parent = target_ref.id;
        }

        if target_ref.parent.exists() {
            let parent = storage.get_node(leaf_ref.parent)?;
            parent.borrow_mut().update_key(leaf_ref.id, new_min_key);

            if leaf_ref.parent != target_ref.parent {
                let parent = storage.get_node(leaf_ref.parent)?;
                if parent.borrow().data_count > 0 {
                    let mut min_key = leaf_ref.keys[0];
                    if !leaf_ref.is_leaf {
                        let first_data = leaf_ref.first_data();

                        let first_child = storage.get_node(first_data.into_id())?;
                        min_key = first_child.borrow().first_key();
                    }
                    rollup_keys(storage, target_ref.parent, first_key, min_key)?;
                }
            }
        }

        return Ok(true);
    }
    return Ok(false);
}
