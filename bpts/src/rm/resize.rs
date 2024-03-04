use crate::{
    node::RcNode,
    nodestorage::NodeStorage,
    rm::{
        move_to::{try_move_to_high, try_move_to_low},
        take_from::{try_take_from_high, try_take_from_low},
    },
    types,
};

pub(in super::super) fn resize(
    storage: &mut dyn NodeStorage,
    target: &RcNode,
    t: usize,
    root: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    println!("resize Id={:?}", target.borrow().id.0);
    let mut target_ref = target.borrow_mut();
    if target_ref.data_count >= t {
        return Ok(root.unwrap());
    }
    if target_ref.keys_count == 0 || target_ref.data_count == 0 {
        if target_ref.parent.is_empty() {
            if target_ref.data_count > 0 && !target_ref.is_leaf {
                storage.erase_node(&target_ref.id);
                let new_root = storage.get_node(target_ref.data[0].into_id())?;
                new_root.borrow_mut().parent.clear();
                return Ok(new_root);
            }
            return Ok(target.clone());
        } else {
            panic!("logic error!");
        }
    }

    let mut link_to_low: Option<RcNode> = None;
    let mut link_to_high: Option<RcNode> = None;

    let mut low_side_size = 0;
    let mut high_side_size = 0;
    let left_exists = target_ref.left.exists();
    let mut parent_of_left = None;
    let mut parent_of_right = None;
    let right_exists = target_ref.right.exists();

    if left_exists {
        let low_side_leaf = storage.get_node(target_ref.left)?;
        link_to_low = Some(low_side_leaf.clone());
        parent_of_left = Some(low_side_leaf.borrow().parent);
        low_side_size = low_side_leaf.borrow().data_count;
    }

    if right_exists {
        let high_side_leaf = storage.get_node(target_ref.right)?;
        link_to_high = Some(high_side_leaf.clone());
        parent_of_right = Some(high_side_leaf.borrow().parent);
        high_side_size = high_side_leaf.borrow().data_count;
    }

    if left_exists
        && (high_side_size <= t
            || (parent_of_left == parent_of_right)
            || (parent_of_left != parent_of_right
                && (parent_of_left == Some(target_ref.parent) || parent_of_right.is_none())))
    {
        // from low side
        let low_side_leaf = link_to_low.clone().unwrap();
        let mut leaf_ref = low_side_leaf.borrow_mut();
        if try_take_from_low(storage, &mut target_ref, &mut leaf_ref, t)? {
            return Ok(root.unwrap());
        }
    }

    if right_exists
        && (low_side_size <= t
            || (parent_of_left == parent_of_right)
            || (parent_of_left != parent_of_right
                && (parent_of_right == Some(target_ref.parent) || parent_of_left.is_none())))
    {
        // from high side
        let high_side_leaf = link_to_high.clone().unwrap();
        let mut leaf_ref = high_side_leaf.borrow_mut();

        if try_take_from_high(storage, &mut target_ref, &mut leaf_ref, t)? {
            return Ok(root.unwrap());
        }
    }

    //try move to brother
    let mut update_parent = false;
    if left_exists
        && ((parent_of_left == parent_of_right)
            || (parent_of_left != parent_of_right
                && (parent_of_left == Some(target_ref.parent) || parent_of_right.is_none())))
    {
        //TODO! already loaded in link_to_low;
        let low_side = link_to_low.clone().unwrap();
        let mut leaf_ref = low_side.borrow_mut();
        update_parent = try_move_to_low(storage, &mut target_ref, &mut leaf_ref, t)?;
    }

    if !update_parent
        && right_exists
        && (right_exists
            && ((parent_of_left == parent_of_right)
                || (parent_of_left != parent_of_right
                    && (parent_of_right == Some(target_ref.parent) || parent_of_left.is_none()))))
    {
        //TODO! already loaded in link_to_high;
        let high_side = link_to_high.unwrap();
        let mut leaf_ref = high_side.borrow_mut();
        update_parent = try_move_to_high(storage, &mut target_ref, &mut leaf_ref, t)?;
    }

    if update_parent && target_ref.parent.exists() {
        //TODO! check result
        let link_to_parent = storage.get_node(target_ref.parent)?;
        if link_to_parent.borrow().keys_count < t {
            return resize(storage, &link_to_parent, t, root);
        }
    }
    return Ok(root.unwrap());
}
