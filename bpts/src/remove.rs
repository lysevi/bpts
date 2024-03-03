use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    read, types, utils,
};

fn erase_key_data(target: &mut Node, key: i32) {
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

fn take_from_low(
    storage: &mut dyn NodeStorage,
    target: &mut Node,
    low_side: &mut Node,
    middle: Option<i32>,
) {
    println!("take_from_low target={:?} low={:?}", target.id, low_side.id);

    //let mut min_key = target.first_key();

    if !target.is_leaf && middle.is_some() {
        println!("take_from_low insert middle");
        utils::insert_to_array(&mut target.keys, 0, middle.unwrap());
        target.keys_count += 1;
    }
    //else
    {
        let max_key = low_side.last_key();
        let max_data = low_side.last_data();

        if !target.is_leaf {
            //TODO! move to resize
            let min_data_node = storage.get_node(max_data.into_id()).unwrap();
            min_data_node.borrow_mut().parent = target.id;
        } else {
            utils::insert_to_array(&mut target.keys, 0, max_key);
            target.keys_count += 1;
        }

        //utils::insert_to_array(&mut target.keys, 0, max_key);
        utils::insert_to_array(&mut target.data, 0, max_data);
        low_side.keys_count -= 1;
        low_side.data_count -= 1;

        //target.keys_count += 1;
        target.data_count += 1;
    }
}

fn take_from_high(target: &mut Node, high_side: &mut Node, middle: Option<i32>) -> i32 {
    println!(
        "take_key_from_high target={:?} high={:?} minKey={}",
        target.id, high_side.id, high_side.keys[0]
    );

    let mut min_key = high_side.first_key();
    let mut result = min_key;
    let min_data = high_side.first_data();
    if !target.is_leaf {
        min_key = middle.unwrap();
        println!(" new minKey={}", min_key);
    }
    {
        let mut position = target.keys_count;
        target.keys[position] = min_key;
        position = target.data_count;
        target.data[position] = min_data;

        utils::remove_with_shift(&mut high_side.keys, 0);
        utils::remove_with_shift(&mut high_side.data, 0);

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

fn move_to_lower(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    low_side_node: &mut Node,
    middle: Option<i32>,
) {
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
    if !target_node.is_leaf {
        let v: Vec<i32> = low_side_node
            .data
            .iter()
            .take(low_side_node.data_count)
            .map(|x| x.into_i32())
            .collect();
        println!("before: {:?}", v);
    }
    {
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
                let node = storage.get_node(node_ptr.into_id()).unwrap();
                node.borrow_mut().parent = low_side_node.id;
            }
        }

        low_side_node.keys_count += target_node.keys_count;
        low_side_node.data_count += target_node.data_count;

        if !target_node.is_leaf {
            let v: Vec<i32> = low_side_node
                .data
                .iter()
                .take(low_side_node.data_count)
                .map(|x| x.into_i32())
                .collect();
            println!("after: {:?}", v);
        }
    }
}

fn move_to_higher(
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

fn rollup_keys(storage: &mut dyn NodeStorage, id: types::Id, key: i32, newkey: i32) {
    println!("rollup tree: Id:{:?} key:{} newkey:{}", id, key, newkey);
    let mut id_of_parent = id;
    while id_of_parent.exists() {
        let node = storage.get_node(id_of_parent).unwrap();
        let mut refn = node.borrow_mut();

        for i in 0..refn.keys_count {
            if refn.keys[i] == key {
                println!("update key in {:?}", refn.id);
                refn.keys[i] = newkey;
                break;
            }
        }

        id_of_parent = refn.parent;
    }
}

fn erase_key(
    storage: &mut dyn NodeStorage,
    target: &RcNode,
    key: i32,
    t: usize,
    root: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    {
        let mut target_ref = target.borrow_mut();
        let first_key = target_ref.keys[0];
        erase_key_data(&mut target_ref, key);
        if target_ref.keys_count > 0 && target_ref.is_leaf && first_key != target_ref.first_key() {
            rollup_keys(
                storage,
                target_ref.parent,
                first_key,
                target_ref.first_key(),
            );
        }
        if target_ref.data_count >= t {
            //update keys in parent
            if first_key != target_ref.keys[0] && target_ref.parent.exists() {
                let parent = storage.get_node(target_ref.parent).unwrap();
                parent
                    .borrow_mut()
                    .update_key(target_ref.id, target_ref.first_key());
            }

            return Ok(root.unwrap());
        }
    }

    return resize(storage, target, t, root);
}

fn resize(
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
                let new_root = storage.get_node(target_ref.data[0].into_id()).unwrap();
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
        let low_side_leaf = storage.get_node(target_ref.left).unwrap();
        link_to_low = Some(low_side_leaf.clone());
        parent_of_left = Some(low_side_leaf.borrow().parent);
        low_side_size = low_side_leaf.borrow().data_count;
    }

    if right_exists {
        let high_side_leaf = storage.get_node(target_ref.right).unwrap();
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
        //TODO! already loaded in link_to_low;
        let low_side_leaf = storage.get_node(target_ref.left).unwrap();
        link_to_low = Some(low_side_leaf.clone());
        let mut leaf_ref = low_side_leaf.borrow_mut();

        if leaf_ref.data_count > t {
            let mut middle: Option<i32> = None;
            let first_key = target_ref.first_key();
            let min_key = leaf_ref.keys[leaf_ref.keys_count - 1];
            if !target_ref.is_leaf {
                let low_data_node = storage.get_node(target_ref.data[0].into_id()).unwrap();
                middle = Some(low_data_node.borrow().first_key());
            }
            take_from_low(storage, &mut target_ref, &mut leaf_ref, middle);
            if !target_ref.is_leaf {
                let taked_id = target_ref.first_data().into_id();
                let taked_node = storage.get_node(taked_id).unwrap();
                taked_node.borrow_mut().parent = target_ref.id;
            }
            if target_ref.parent.exists() {
                //TODO! check result
                let link_to_parent = storage.get_node(target_ref.parent).unwrap();
                link_to_parent
                    .borrow_mut()
                    .update_key(target_ref.id, min_key);

                if leaf_ref.parent != target_ref.parent {
                    rollup_keys(storage, target_ref.parent, first_key, min_key);
                }
            }

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
        //TODO! already loaded in link_to_high;
        let high_side_leaf = storage.get_node(target_ref.right).unwrap();
        link_to_high = Some(high_side_leaf.clone());
        let mut leaf_ref = high_side_leaf.borrow_mut();

        let first_key = leaf_ref.keys[0];
        if leaf_ref.data_count > t {
            let min_key = leaf_ref.keys[0];
            let mut middle: Option<i32> = None;
            if !target_ref.is_leaf {
                let parent = storage.get_node(leaf_ref.parent).unwrap();
                middle = parent.borrow().find_key(min_key);
            }

            let new_min_key = take_from_high(&mut target_ref, &mut leaf_ref, middle);
            if !target_ref.is_leaf {
                let taked_id = target_ref.last_data().into_id();
                let taked_node = storage.get_node(taked_id).unwrap();
                taked_node.borrow_mut().parent = target_ref.id;
            }

            if target_ref.parent.exists() {
                //TODO! check result
                let parent = storage.get_node(leaf_ref.parent).unwrap();
                parent.borrow_mut().update_key(leaf_ref.id, new_min_key);

                if leaf_ref.parent != target_ref.parent {
                    let parent = storage.get_node(target_ref.parent).unwrap();
                    if parent.borrow().data_count > 0 {
                        let mut min_key = leaf_ref.keys[0];
                        if !leaf_ref.is_leaf {
                            let first_data = leaf_ref.first_data();

                            let first_child = storage.get_node(first_data.into_id()).unwrap();
                            min_key = first_child.borrow().first_key();
                        }
                        rollup_keys(storage, target_ref.parent, first_key, min_key);
                    }
                }
            }

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
        let low_side = if link_to_low.is_some() {
            link_to_low.unwrap()
        } else {
            storage.get_node(target_ref.left).unwrap()
        };
        let mut leaf_ref = low_side.borrow_mut();

        if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
            let min_key = target_ref.keys[0];
            let mut middle: Option<i32> = None;

            if target_ref.parent.exists() {
                let parent = storage.get_node(target_ref.parent).unwrap();
                if !target_ref.is_leaf {
                    middle = parent.borrow().find_key(min_key);
                }
                parent.borrow_mut().erase_link(target_ref.id);
            }
            // if !target_ref.is_leaf && target_ref.data_count > 1 {
            //     let child1 = storage.get_node(target_ref.data[0].into_id()).unwrap();
            //     let cf = child1.borrow();
            //     middle = Some(cf.first_key());
            // }
            let first_key = leaf_ref.first_key();
            move_to_lower(storage, &mut target_ref, &mut leaf_ref, middle);
            storage.erase_node(&target_ref.id);

            if target_ref.parent.exists() {
                if leaf_ref.parent != target_ref.parent {
                    let parent = storage.get_node(target_ref.parent).unwrap();
                    if parent.borrow().data_count > 0 {
                        let mut min_key = leaf_ref.keys[0];
                        if !leaf_ref.is_leaf {
                            //TODO check. if leaf_ref is leaf?
                            let first_data = leaf_ref.first_data();

                            let first_child = storage.get_node(first_data.into_id()).unwrap();
                            min_key = first_child.borrow().first_key();
                        }
                        rollup_keys(storage, target_ref.parent, first_key, min_key);
                    }
                }
            }

            //TODO! check result;
            if target_ref.right.exists() {
                let right_side = storage.get_node(target_ref.right).unwrap();
                right_side.borrow_mut().left = target_ref.left;
                leaf_ref.right = target_ref.right;
            }
            leaf_ref.right = target_ref.right;
            update_parent = true;
        }
    }

    if !update_parent
        && right_exists
        && (right_exists
            && ((parent_of_left == parent_of_right)
                || (parent_of_left != parent_of_right
                    && (parent_of_right == Some(target_ref.parent) || parent_of_left.is_none()))))
    {
        //TODO! already loaded in link_to_high;
        let high_side = if link_to_high.is_some() {
            link_to_high.unwrap()
        } else {
            storage.get_node(target_ref.right).unwrap()
        };

        let mut leaf_ref = high_side.borrow_mut();
        let mut high_first_key = leaf_ref.first_key();

        if (leaf_ref.keys_count + target_ref.keys_count) < 2 * t {
            let min_key = leaf_ref.keys[0];
            let mut middle: Option<i32> = None;
            if target_ref.parent.exists() {
                let parent = storage.get_node(leaf_ref.parent).unwrap();
                if !target_ref.is_leaf {
                    middle = parent.borrow().find_key(min_key);
                }
                parent.borrow_mut().erase_link(target_ref.id);
            }

            move_to_higher(storage, &mut target_ref, &mut leaf_ref, middle);

            leaf_ref.left = target_ref.left;
            storage.erase_node(&target_ref.id);

            if target_ref.parent.exists() {
                if leaf_ref.parent != target_ref.parent {
                    if !leaf_ref.is_leaf {
                        let first_data = leaf_ref.first_data();

                        let first_child = storage.get_node(first_data.into_id()).unwrap();
                        high_first_key = first_child.borrow().first_key();
                    }
                    //TODO checks;
                    rollup_keys(
                        storage,
                        leaf_ref.parent,
                        high_first_key,
                        leaf_ref.first_key(),
                    );
                }
            }

            if target_ref.left.exists() {
                let left_side = storage.get_node(target_ref.left).unwrap();
                left_side.borrow_mut().right = target_ref.right;
                leaf_ref.left = target_ref.left;
            }

            update_parent = true;
        }
    }

    if update_parent && target_ref.parent.exists() {
        //TODO! check result
        let link_to_parent = storage.get_node(target_ref.parent).unwrap();
        if link_to_parent.borrow().keys_count < t {
            return resize(storage, &link_to_parent, t, root);
        }
    }

    return Ok(root.unwrap());
}

pub fn remove_key(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
    t: usize,
) -> Result<RcNode, types::Error> {
    let target_node: RcNode;

    let scan_result = read::scan(storage, &root, key);
    if scan_result.is_err() {
        return Err(scan_result.err().unwrap());
    } else {
        target_node = scan_result.unwrap();
    }

    {
        let r = target_node.borrow();
        println!(
            "remove from {:?} ({},{},{})",
            r.id, r.left.0, r.right.0, r.parent.0
        );
    }
    return erase_key(storage, &target_node, key, t, Some(root.clone()));
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::insert::insert;
    use crate::mocks::MockNodeStorage;
    use crate::node::Node;
    use crate::read::{find, map, map_rev};
    use crate::rec::Record;

    fn make_tree(nodes_count: usize, t: usize) -> (MockNodeStorage, RcNode, Vec<i32>) {
        let mut root_node = Node::new_leaf_with_size(types::Id(1), t);

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let mut key: i32 = 1;
        let mut keys = Vec::new();
        while storage.size() <= nodes_count {
            key += 1;
            let res = insert(&mut storage, &root_node, key, &Record::from_i32(key), t);
            keys.push(key);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }
        }
        return (storage, root_node, keys);
    }

    #[test]
    fn remove_from_leaf() {
        let leaf = Node::new_leaf(
            types::Id(1),
            vec![1, 2, 3, 4, 5, 6],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
                Record::from_u8(5),
                Record::from_u8(6),
            ],
            6,
            6,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf);

        let result = erase_key(&mut storage, &leaf, 2, 3, Some(leaf.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![1, 3, 4, 5, 6, 2]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_u8(1),
                    Record::from_u8(3),
                    Record::from_u8(4),
                    Record::from_u8(5),
                    Record::from_u8(6),
                    Record::from_u8(2),
                ]
            );
            assert_eq!(ref_root.keys_count, 5);
            assert_eq!(ref_root.data_count, 5);
        }
    }

    #[test]
    fn remove_from_leaf_update_parent() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        let leaf1 = Node::new_leaf(
            types::Id(1),
            vec![1, 2, 3, 4],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
            ],
            4,
            4,
        );
        storage.add_node(&leaf1);

        let leaf2 = Node::new_leaf(
            types::Id(2),
            vec![5, 6, 7, 8],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
                Record::from_u8(8),
            ],
            4,
            4,
        );
        storage.add_node(&leaf2);

        let root = Node::new_root(
            types::Id(3),
            vec![5, 0, 0, 0],
            vec![
                Record::from_id(types::Id(1)),
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id::empty()),
                Record::from_id(types::Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);
        leaf1.borrow_mut().parent = root.borrow().id;
        leaf2.borrow_mut().parent = root.borrow().id;

        let result = erase_key(&mut storage, &leaf2, 5, 3, Some(root.clone()));
        assert!(result.is_ok());
        {
            let newroot = result.unwrap();
            let ref_root = newroot.borrow();
            assert_eq!(ref_root.id, root.borrow().id);
            assert_eq!(ref_root.keys, vec![6, 0, 0, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_id(types::Id(1)),
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::Id::empty()),
                    Record::from_id(types::Id::empty()),
                ]
            );
            assert_eq!(ref_root.keys_count, 1);
            assert_eq!(ref_root.data_count, 2);
        }

        {
            let ref_leaf2 = leaf2.borrow_mut();
            assert_eq!(ref_leaf2.keys, vec![6, 7, 8, 5]);
            assert_eq!(
                ref_leaf2.data,
                vec![
                    Record::from_u8(6),
                    Record::from_u8(7),
                    Record::from_u8(8),
                    Record::from_u8(5),
                ]
            );
            assert_eq!(ref_leaf2.keys_count, 3);
            assert_eq!(ref_leaf2.data_count, 3);
        }
    }

    #[test]
    fn remove_from_leaf_take_from_lower() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();

        let root = Node::new_root(
            types::Id(3),
            vec![5, 0, 0, 0],
            vec![
                Record::from_id(types::Id(1)),
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id::empty()),
                Record::from_id(types::Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);

        let leaf_high = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
                Record::from_u8(0),
            ],
            3,
            3,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            types::Id(2),
            vec![1, 2, 3, 4],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
            ],
            4,
            4,
        );
        storage.add_node(&leaf_low);

        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = erase_key(&mut storage, &leaf_high, 6, 3, Some(root.clone()));
        assert!(result.is_ok());

        {
            let ref_node: std::cell::RefMut<'_, Node> = root.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 0, 0, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_id(types::Id(1)),
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::Id::empty()),
                    Record::from_id(types::Id::empty()),
                ]
            );
            assert_eq!(ref_node.keys_count, 1);
            assert_eq!(ref_node.data_count, 2);
        }

        {
            let ref_node = leaf_high.borrow_mut();
            assert_eq!(ref_node.keys, vec![4, 5, 7, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(4),
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }

        {
            let ref_node = leaf_low.borrow_mut();
            assert_eq!(ref_node.keys, vec![1, 2, 3, 4]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(1),
                    Record::from_u8(2),
                    Record::from_u8(3),
                    Record::from_u8(4),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
    }

    #[test]
    fn remove_from_leaf_take_from_high() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        let root = Node::new_root(
            types::Id(3),
            vec![9, 0, 0, 0],
            vec![
                Record::from_id(types::Id(1)),
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id::empty()),
                Record::from_id(types::Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);

        let leaf_low = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
                Record::from_u8(0),
            ],
            3,
            3,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            types::Id(2),
            vec![9, 10, 11, 12],
            vec![
                Record::from_u8(9),
                Record::from_u8(10),
                Record::from_u8(11),
                Record::from_u8(12),
            ],
            4,
            4,
        );
        storage.add_node(&leaf_high);
        leaf_low.borrow_mut().right = leaf_high.borrow().id;
        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;

        let result = erase_key(&mut storage, &leaf_low, 6, 3, Some(root.clone()));
        assert!(result.is_ok());

        {
            let newroot = result.unwrap();
            let ref_root = newroot.borrow();
            assert_eq!(ref_root.id, root.borrow().id);
            assert_eq!(ref_root.keys, vec![10, 0, 0, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_id(types::Id(1)),
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::Id::empty()),
                    Record::from_id(types::Id::empty()),
                ]
            );
            assert_eq!(ref_root.keys_count, 1);
            assert_eq!(ref_root.data_count, 2);
        }

        {
            let ref_node = leaf_low.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 7, 9, 6]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(9),
                    Record::from_u8(6),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }

        {
            let ref_node = leaf_high.borrow_mut();
            assert_eq!(ref_node.keys, vec![10, 11, 12, 9]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(10),
                    Record::from_u8(11),
                    Record::from_u8(12),
                    Record::from_u8(9),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
    }

    #[test]
    fn remove_from_leaf_move_to_lower() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();

        let leaf_high = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            types::Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_low);
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = erase_key(&mut storage, &leaf_high, 6, 3, Some(leaf_high.clone()));
        assert!(result.is_ok());

        assert!(!storage.is_exists(leaf_high.borrow().id));
        {
            let ref_node = leaf_low.borrow_mut();
            assert_eq!(ref_node.keys, vec![1, 2, 5, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(1),
                    Record::from_u8(2),
                    Record::from_u8(5),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
    }

    #[test]
    fn remove_from_leaf_move_to_high() {
        let leaf_low = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
                Record::from_u8(0),
            ],
            3,
            3,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            types::Id(2),
            vec![9, 10, 0, 0],
            vec![
                Record::from_u8(9),
                Record::from_u8(10),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_high);
        leaf_low.borrow_mut().right = leaf_high.borrow().id;

        let result = erase_key(&mut storage, &leaf_low, 6, 3, Some(leaf_low.clone()));
        assert!(result.is_ok());

        assert!(!storage.is_exists(leaf_low.borrow().id));
        {
            let ref_node = leaf_high.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 7, 9, 10]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(9),
                    Record::from_u8(10),
                ]
            );
            assert_eq!(ref_node.keys_count, 4);
            assert_eq!(ref_node.data_count, 4);
        }
    }

    #[test]
    #[should_panic]
    fn remove_from_node_first() {
        let node = Node::new_root(
            types::Id(1),
            vec![5, 8, 0],
            vec![Record::from_u8(1), Record::from_u8(5), Record::from_u8(10)],
            2,
            3,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&node);

        let result = erase_key(&mut storage, &node, 5, 3, Some(node.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![8, 0, 5]);
            assert_eq!(
                ref_root.data,
                vec![Record::from_u8(1), Record::from_u8(10), Record::from_u8(5),]
            );
            assert_eq!(ref_root.keys_count, 1);
            assert_eq!(ref_root.data_count, 2);
        }
    }

    #[test]
    fn remove_from_leaf_move_to_lower_update_parent() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();

        let root = Node::new_root(
            types::Id(4),
            vec![5, 12, 0, 0],
            vec![
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id(1)),
                Record::from_id(types::Id(3)),
                Record::from_id(types::Id::empty()),
            ],
            2,
            3,
        );
        storage.add_node(&root);

        let leaf_extra = Node::new_leaf(
            types::Id(3),
            vec![12, 15, 0, 0],
            vec![
                Record::from_u8(12),
                Record::from_u8(15),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_high = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            types::Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_low);
        leaf_high.borrow_mut().left = leaf_low.borrow().id;
        leaf_low.borrow_mut().right = leaf_high.borrow().id;

        leaf_high.borrow_mut().right = leaf_extra.borrow().id;
        leaf_extra.borrow_mut().left = leaf_high.borrow().id;

        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;
        leaf_extra.borrow_mut().parent = root.borrow().id;
        let result = erase_key(&mut storage, &leaf_high, 6, 3, Some(root.clone()));
        assert!(result.is_ok());
        let new_root = result.unwrap();
        assert_eq!(new_root.borrow().id, root.borrow().id);

        assert!(!storage.is_exists(leaf_high.borrow().id));
        {
            let ref_node = leaf_low.borrow_mut();
            assert_eq!(ref_node.keys, vec![1, 2, 5, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(1),
                    Record::from_u8(2),
                    Record::from_u8(5),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);

            assert_eq!(ref_node.right, leaf_extra.borrow().id);
            assert_eq!(leaf_extra.borrow().left, ref_node.id);
        }

        {
            let ref_node = root.borrow_mut();
            assert_eq!(ref_node.keys, vec![12, 0, 0, 5]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::Id(3)),
                    Record::from_id(types::Id::empty()),
                    Record::from_id(types::Id(1)),
                ]
            );
            assert_eq!(ref_node.keys_count, 1);
            assert_eq!(ref_node.data_count, 2);
        }
    }

    #[test]
    fn remove_from_leaf_move_to_high_update_parent() {
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        /*
              9            15
         5 6    9 10, 0, 0   15 16
        */
        let root = Node::new_root(
            types::Id(3),
            vec![9, 15, 0, 0],
            vec![
                Record::from_id(types::Id(1)),
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id(4)),
                Record::from_id(types::Id::empty()),
            ],
            2,
            3,
        );
        storage.add_node(&root);

        let leaf_extra = Node::new_leaf(
            types::Id(4),
            vec![15, 16, 0, 0],
            vec![
                Record::from_u8(15),
                Record::from_u8(16),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_low = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            types::Id(2),
            vec![9, 10, 0, 0],
            vec![
                Record::from_u8(9),
                Record::from_u8(10),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_high);

        leaf_low.borrow_mut().right = leaf_high.borrow().id;
        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;
        leaf_extra.borrow_mut().parent = root.borrow().id;

        let result = erase_key(&mut storage, &leaf_low, 6, 2, Some(root.clone()));
        assert!(result.is_ok());

        let low_id = leaf_low.borrow().id;
        assert!(!storage.is_exists(low_id));
        {
            let newroot = result.unwrap();
            let ref_root = newroot.borrow();
            assert_eq!(ref_root.id, root.borrow().id);
            assert_eq!(ref_root.keys, vec![15, 0, 0, 9]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::Id(4)),
                    Record::from_id(types::Id::empty()),
                    Record::from_id(types::Id(1)),
                ]
            );
            assert_eq!(ref_root.keys_count, 1);
            assert_eq!(ref_root.data_count, 2);
        }

        {
            let ref_node = leaf_high.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 9, 10, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_u8(5),
                    Record::from_u8(9),
                    Record::from_u8(10),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
    }

    fn many_inserts(t: usize, maxnodes: usize) {
        for hight in 3..maxnodes {
            // let hight = 22;
            let (mut storage, mut root_node, keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            for i in 2..=key {
                let find_res = find(&mut storage, &root_node, i);
                assert!(find_res.is_ok());
                assert_eq!(find_res.unwrap().into_i32(), i);
                // /                println!("remove {:?}", i);

                let str_before =
                    storage.to_string(root_node.clone(), true, &String::from("before"));

                let remove_res = remove_key(&mut storage, &root_node, i, t);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();

                let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

                let mut mapped_values = Vec::new();
                map(&mut storage, &root_node, i, key, &mut |k, v| {
                    assert_eq!(v.into_i32(), k);
                    mapped_values.push(k);
                })
                .unwrap();

                for i in 1..mapped_values.len() {
                    if mapped_values[i - 1] >= mapped_values[i] {
                        println!("bad order");
                        MockNodeStorage::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    assert!(i == key);
                    break;
                }
                let find_res = find(&mut storage, &root_node, i);
                if find_res.is_err() {
                    break;
                }
                assert!(!find_res.is_err());
                // print_state(&str_before, &str_after);
                // break;
                for k in (i + 1)..key {
                    //println!("? {:?}", k);
                    // if k == 14 {
                    //     println!("!!");
                    // }
                    let find_res = find(&mut storage, &root_node, k);
                    if find_res.is_err() {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_ok());
                    let d = find_res.unwrap();
                    if d.into_i32() != k {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), k);
                }
            }
        }

        //TODO check map map_rev
    }

    fn many_inserts_rev(t: usize, maxnodes: usize) {
        for hight in 3..maxnodes {
            let (mut storage, mut root_node, keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            for i in (2..=key).rev() {
                let find_res = find(&mut storage, &root_node, i);
                assert!(find_res.is_ok());
                assert_eq!(find_res.unwrap().into_i32(), i);
                println!(">> remove {:?}", i);
                let str_before =
                    storage.to_string(root_node.clone(), true, &String::from("before"));

                let remove_res = remove_key(&mut storage, &root_node, i, t);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();
                let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

                if root_node.borrow().is_empty() && i == 2 {
                    break;
                }
                let mut mapped_values = Vec::new();
                map_rev(&mut storage, &root_node, i, key, &mut |k, v| {
                    assert_eq!(v.into_i32(), k);
                    mapped_values.push(k);
                })
                .unwrap();

                for i in 1..mapped_values.len() {
                    if mapped_values[i - 1] <= mapped_values[i] {
                        println!("bad order");
                        MockNodeStorage::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    assert!(i == key);
                    break;
                }
                // let find_res = find(&mut storage, &root_node, i);
                // if find_res.is_err() {
                //     assert!(find_res.is_ok());
                //     break;
                // }
                // assert!(!find_res.is_err());

                for k in 2..i {
                    // println!("? {:?}", k);
                    // if k == 14 {
                    //     println!("!!");
                    // }
                    let find_res = find(&mut storage, &root_node, k);
                    if find_res.is_err() {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_ok());
                    let d = find_res.unwrap();
                    if d.into_i32() != k {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), k);
                }
            }
        }

        //TODO check map map_fwd
    }

    fn many_inserts_middle_range(t: usize, maxnodes: usize) {
        for hight in 3..maxnodes {
            //    let hight = 21;
            let (mut storage, mut root_node, mut keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            /*let first = &keys[0..keys.len() / 2];
            let last = &keys[keys.len() / 2..];
            let new_key_list = [last, first].concat();

            for i in new_key_list */

            while keys.len() > 0 {
                let i = keys[keys.len() / 2];
                keys.remove(keys.len() / 2);
                let find_res = find(&mut storage, &root_node, i);
                assert!(find_res.is_ok());
                assert_eq!(find_res.unwrap().into_i32(), i);
                println!(">> {} {} remove {:?} size: {}", hight, t, i, storage.size());
                // if i == 29 {
                //     println!("!");
                // }
                let str_before =
                    storage.to_string(root_node.clone(), true, &String::from("before"));

                let remove_res = remove_key(&mut storage, &root_node, i, t);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();

                let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));
                // if i == 11 {
                //     print_state(&str_before, &str_after);
                // }
                //                break;
                let mut mapped_values = Vec::new();
                if keys.len() > 2 {
                    map(
                        &mut storage,
                        &root_node,
                        i,
                        *keys.last().unwrap(),
                        &mut |k, v| {
                            assert_eq!(v.into_i32(), k);
                            mapped_values.push(k);
                        },
                    )
                    .unwrap();
                }

                for i in 1..mapped_values.len() {
                    if mapped_values[i - 1] >= mapped_values[i] {
                        println!("bad order");
                        MockNodeStorage::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    break;
                }
                // let find_res = find(&mut storage, &root_node, i);
                // if find_res.is_err() {
                //     break;
                // }
                // assert!(!find_res.is_err());
                // print_state(&str_before, &str_after);
                // break;
                for k in &keys {
                    // println!("? {:?}", k);
                    // if *k == 20 {
                    //     println!("!!");
                    // }
                    let find_res = find(&mut storage, &root_node, *k);
                    if find_res.is_err() {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_ok());
                    let d = find_res.unwrap();
                    if d.into_i32() != *k {
                        MockNodeStorage::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), *k);
                }
            }
        }

        //TODO check map map_rev
    }

    #[test]
    fn many_inserts_3_22() {
        many_inserts(3, 22);
    }

    #[test]
    fn many_inserts_7_22() {
        many_inserts(7, 22);
    }

    #[test]
    fn many_inserts_16_10() {
        many_inserts(16, 22);
    }

    #[test]
    fn many_inserts_rev_3_22() {
        many_inserts_rev(3, 22);
    }

    #[test]
    fn many_inserts_rev_7_22() {
        many_inserts_rev(7, 22);
    }

    #[test]
    fn many_inserts_rev_16_22() {
        many_inserts_rev(16, 22);
    }

    #[test]
    fn many_inserts_middle_range_3_22() {
        many_inserts_middle_range(3, 22);
    }

    #[test]
    fn many_inserts_middle_range_7_22() {
        many_inserts_middle_range(7, 22);
    }

    #[test]
    fn remove_from_middle_leaf() {
        let (mut storage, mut root_node, _keys) = make_tree(7, 3);

        let res = insert(&mut storage, &root_node, 1, &Record::from_i32(1), 3);
        root_node = res.unwrap();

        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));

        let remove_res = remove_key(&mut storage, &root_node, 5, 3);
        assert!(remove_res.is_ok());
        root_node = remove_res.unwrap();

        let str_after = storage.to_string(root_node.clone(), true, &String::from("after"));

        // {
        //     print_state(&str_before, &str_after);
        // }

        for i in 1..19 {
            if i == 5 {
                continue;
            }
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().into_i32(), i);
        }
    }

    #[test]
    fn remove_with_take_high_leaf_diff_parent() {
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
            let res = resize(&mut storage, &node, 3, Some(root_node.clone()));
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
            assert_eq!(find_res.unwrap().into_i32(), i);
        }
    }

    #[test]
    fn remove_with_take_low_leaf_diff_parent() {
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
            let res = resize(&mut storage, &node, 3, Some(root_node.clone()));
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

        for i in [
            2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 22, 23, 26, 27, 28, 29, 30,
            31, 32, 33,
        ] {
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                MockNodeStorage::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().into_i32(), i);
        }
    }
}
