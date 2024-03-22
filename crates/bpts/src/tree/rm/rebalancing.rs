use crate::tree::{
    node::RcNode,
    nodestorage::NodeStorage,
    rm::{
        move_to::{try_move_to_high, try_move_to_low},
        take_from::{try_take_from_high, try_take_from_low},
    },
};

pub(in super::super) fn rebalancing<Storage: NodeStorage>(
    storage: &mut Storage,
    target: &RcNode,
    root: Option<RcNode>,
) -> crate::Result<RcNode> {
    println!("resize Id={:?}", target.borrow().id.0);
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
            return rebalancing(storage, &link_to_parent, root);
        }
    }
    return Ok(root.unwrap());
}

#[cfg(test)]
mod tests {
    use super::super::super::remove::tests::make_tree;
    use crate::tree::debug;
    use crate::tree::nodestorage::NodeStorage;
    use crate::tree::read::find;
    use crate::{prelude::*, types::Id};
    use std::collections::HashSet;
    #[test]
    #[ignore]
    fn remove_with_take_high_leaf_diff_parent() -> crate::Result<()> {
        let (mut storage, mut root_node, _keys) = make_tree(10, 4);

        let mut keyset: HashSet<u32> = HashSet::from_iter(_keys.iter().cloned());

        let str_before =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("before"));
        {
            let node = storage.get_node(Id(4))?;
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
            keyset.remove(&12);
            keyset.remove(&13);
        }

        {
            let node = storage.get_node(Id(5))?;
            {
                let mut nr = node.borrow_mut();
                nr.keys_count -= 2;
                nr.data_count -= 2;
            }
            keyset.remove(&16);
            keyset.remove(&17);
            storage.change_t(3);
            let res = super::rebalancing(&mut storage, &node, Some(root_node.clone()));
            root_node = res.unwrap()
        }
        {
            let node = storage.get_node(Id(10)).unwrap();
            assert_eq!(node.borrow().keys[0], 19);
        }

        let str_after =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

        {
            debug::print_states(&[&str_before, &str_after]);
        }

        for i in keyset {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                debug::print_states(&[&str_before, &str_after]);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_u32(), i);
        }
        return Ok(());
    }

    #[test]
    #[ignore]
    fn remove_with_take_low_leaf_diff_parent() -> Result<()> {
        let (mut storage, mut root_node, _keys) = make_tree(10, 4);

        let mut keyset: HashSet<u32> = HashSet::from_iter(_keys.iter().cloned());

        let str_before =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(Id(7)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
            keyset.remove(&24);
            keyset.remove(&25);
        }

        {
            let node = storage.get_node(Id(6)).unwrap();
            {
                let mut nr = node.borrow_mut();
                nr.keys_count -= 2;
                nr.data_count -= 2;
            }
            keyset.remove(&20);
            keyset.remove(&21);
            storage.change_t(3);
            let res = super::rebalancing(&mut storage, &node, Some(root_node.clone()));
            root_node = res.unwrap()
        }
        {
            let node = storage.get_node(Id(10)).unwrap();
            assert_eq!(node.borrow().keys[0], 17);
        }
        let str_after =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

        {
            debug::print_states(&[&str_before, &str_after]);
        }

        for i in keyset {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                debug::print_states(&[&str_before, &str_after]);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_u32(), i);
        }
        return Ok(());
    }

    #[test]
    #[ignore]
    fn remove_with_take_low_node_diff_parent() -> Result<()> {
        let (mut storage, mut root_node, _keys) = make_tree(50, 4);

        let str_before =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(Id(31)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 1;
            nr.data_count -= 1;
        }

        {
            let node = storage.get_node(Id(26)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
        }
        let node = storage.get_node(Id(26)).unwrap();
        storage.change_t(3);
        let res = super::rebalancing(&mut storage, &node, Some(root_node.clone()));
        root_node = res.unwrap();
        let str_after =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

        {
            debug::print_states(&[&str_before, &str_after]);
        }

        for i in [2, 157, 58, 59, 60, 61, 62, 63, 64, 65] {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                debug::print_states(&[&str_before, &str_after]);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_u32(), i);
        }
        return Ok(());
    }

    #[test]
    #[ignore]
    fn remove_with_take_high_node_diff_parent() -> Result<()> {
        let (mut storage, mut root_node, _keys) = make_tree(50, 4);

        let str_before =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("before"));

        {
            let node = storage.get_node(Id(16)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 1;
            nr.data_count -= 1;
        }

        {
            let node = storage.get_node(Id(21)).unwrap();
            let mut nr = node.borrow_mut();
            nr.keys_count -= 2;
            nr.data_count -= 2;
        }
        let node = storage.get_node(Id(21)).unwrap();
        storage.change_t(3);
        let res = super::rebalancing(&mut storage, &node, Some(root_node.clone()));
        root_node = res.unwrap();
        let str_after =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

        {
            debug::print_states(&[&str_before, &str_after]);
        }

        for i in [2, 66, 67, 68, 69, 70, 71, 157] {
            let find_res = find(&mut storage, &root_node, i)?;
            if find_res.is_none() {
                debug::print_states(&[&str_before, &str_after]);
            }
            assert!(find_res.is_some());
            assert_eq!(find_res.unwrap().into_u32(), i);
        }
        return Ok(());
    }
}
