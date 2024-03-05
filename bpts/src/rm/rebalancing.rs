use crate::{
    node::RcNode,
    nodestorage::NodeStorage,
    rm::{
        move_to::{try_move_to_high, try_move_to_low},
        take_from::{try_take_from_high, try_take_from_low},
    },
    types,
};

pub(in super::super) fn rebalancing(
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
            return rebalancing(storage, &link_to_parent, t, root);
        }
    }
    return Ok(root.unwrap());
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::super::remove::tests::make_tree;
    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::read::find;

    #[test]
    fn remove_with_take_high_leaf_diff_parent() -> Result<(), types::Error> {
        let (mut storage, mut root_node, _keys) = make_tree(10, 4);

        let mut keyset: HashSet<i32> = HashSet::from_iter(_keys.iter().cloned());

        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));
        {
            let node = storage.get_node(types::Id(4)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
            keyset.remove(&12);
            keyset.remove(&13);
        }

        {
            let node = storage.get_node(types::Id(5)).unwrap();
            {
                let mut nr = node.borrow_mut();
                nr.keys_count -= 2;
                nr.data_count -= 2;
            }
            keyset.remove(&16);
            keyset.remove(&17);
            let res = rebalancing(&mut storage, &node, 3, Some(root_node.clone()));
            root_node = res.unwrap()
        }
        {
            let node = storage.get_node(types::Id(10)).unwrap();
            assert_eq!(node.borrow().keys[0], 19);
        }

        let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

        {
            MockNodeStorage::print_state(&str_before, &str_after);
        }

        for i in keyset {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_i32(), i);
        }
        return Ok(());
    }

    #[test]
    fn remove_with_take_low_leaf_diff_parent() -> Result<(), types::Error> {
        let (mut storage, mut root_node, _keys) = make_tree(10, 4);

        let mut keyset: HashSet<i32> = HashSet::from_iter(_keys.iter().cloned());

        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(types::Id(7)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
            keyset.remove(&24);
            keyset.remove(&25);
        }

        {
            let node = storage.get_node(types::Id(6)).unwrap();
            {
                let mut nr = node.borrow_mut();
                nr.keys_count -= 2;
                nr.data_count -= 2;
            }
            keyset.remove(&20);
            keyset.remove(&21);
            let res = rebalancing(&mut storage, &node, 3, Some(root_node.clone()));
            root_node = res.unwrap()
        }
        {
            let node = storage.get_node(types::Id(10)).unwrap();
            assert_eq!(node.borrow().keys[0], 17);
        }
        let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

        {
            MockNodeStorage::print_state(&str_before, &str_after);
        }

        for i in keyset {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_i32(), i);
        }
        return Ok(());
    }

    #[test]
    fn remove_with_take_low_node_diff_parent() -> Result<(), types::Error> {
        let (mut storage, mut root_node, _keys) = make_tree(50, 4);

        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(types::Id(31)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 1;
            nr.data_count -= 1;
        }

        {
            let node = storage.get_node(types::Id(26)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
        }
        let node = storage.get_node(types::Id(26)).unwrap();
        let res = rebalancing(&mut storage, &node, 3, Some(root_node.clone()));
        root_node = res.unwrap();
        let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

        {
            MockNodeStorage::print_state(&str_before, &str_after);
        }

        for i in [2, 157, 58, 59, 60, 61, 62, 63, 64, 65] {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_i32(), i);
        }
        return Ok(());
    }

    #[test]
    fn remove_with_take_high_node_diff_parent() -> Result<(), types::Error> {
        let (mut storage, mut root_node, _keys) = make_tree(50, 4);

        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(types::Id(16)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 1;
            nr.data_count -= 1;
        }

        {
            let node = storage.get_node(types::Id(21)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
        }
        let node = storage.get_node(types::Id(21)).unwrap();
        let res = rebalancing(&mut storage, &node, 3, Some(root_node.clone()));
        root_node = res.unwrap();
        let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

        {
            MockNodeStorage::print_state(&str_before, &str_after);
        }

        for i in [2, 66, 67, 68, 69, 70, 71, 157] {
            let find_res = find(&mut storage, &root_node, i)?;
            if find_res.is_none() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_some());
            assert_eq!(find_res.unwrap().into_i32(), i);
        }
        return Ok(());
    }
}
