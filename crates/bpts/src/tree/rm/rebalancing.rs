use crate::{
    tree::{
        node::RcNode,
        nodestorage::NodeStorage,
        rm::{
            move_to::{try_move_to_high, try_move_to_low},
            take_from::{try_take_from_high, try_take_from_low},
        },
    },
    verbose,
};

pub(in super::super) fn rebalancing<Storage: NodeStorage>(
    storage: &mut Storage,
    target: &RcNode,
    root: Option<RcNode>,
) -> crate::Result<RcNode> {
    verbose!("resize Id={:?}", target.borrow().id.0);
    let mut target_ref = target.borrow_mut();
    let mut t = storage.get_params().get_min_size_leaf();
    if !target_ref.is_leaf {
        t = if target_ref.parent.is_empty() {
            storage.get_params().get_min_size_root()
        } else {
            storage.get_params().get_min_size_node()
        }
    }
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

    let left_exists = target_ref.left.exists();
    let right_exists = target_ref.right.exists();

    if left_exists {
        let low_side_leaf = storage.get_node(target_ref.left)?;
        if low_side_leaf.borrow().parent == target_ref.parent {
            link_to_low = Some(low_side_leaf.clone());
        }
    }

    if right_exists {
        let high_side_leaf = storage.get_node(target_ref.right)?;
        if high_side_leaf.borrow().parent == target_ref.parent {
            link_to_high = Some(high_side_leaf.clone());
        }
    }

    if link_to_low.is_some() {
        // from low side
        let low_side_leaf = link_to_low.clone().unwrap();
        let mut leaf_ref = low_side_leaf.borrow_mut();
        if try_take_from_low(storage, &mut target_ref, &mut leaf_ref, t)? {
            return Ok(root.unwrap());
        }
    }

    if link_to_high.is_some() {
        // from high side
        let high_side_leaf = link_to_high.clone().unwrap();
        let mut leaf_ref = high_side_leaf.borrow_mut();

        if try_take_from_high(storage, &mut target_ref, &mut leaf_ref, t)? {
            return Ok(root.unwrap());
        }
    }

    //try move to brother
    let mut update_parent = false;
    if link_to_low.is_some() {
        let low_side = link_to_low.clone().unwrap();
        let mut leaf_ref = low_side.borrow_mut();
        update_parent = try_move_to_low(storage, &mut target_ref, &mut leaf_ref, t)?;
    }

    if !update_parent && link_to_high.is_some() {
        let high_side = link_to_high.unwrap();
        let mut leaf_ref = high_side.borrow_mut();
        update_parent = try_move_to_high(storage, &mut target_ref, &mut leaf_ref, t)?;
    }

    if update_parent && target_ref.parent.exists() {
        let link_to_parent = storage.get_node(target_ref.parent)?;
        if link_to_parent.borrow().keys_count < t {
            storage.mark_as_changed(target_ref.parent);
            return rebalancing(storage, &link_to_parent, root);
        }
    }
    return Ok(root.unwrap());
}
