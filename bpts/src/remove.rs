use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    read, types, utils,
};

enum ErasePosition {
    First,
    Other,
}

fn erase_key_data(target_node: &mut Node, key: i32) -> ErasePosition {
    let is_leaf = target_node.is_leaf;

    if !is_leaf {
        println!("erase_key_data node from={:?} key={}", target_node.id, key);
        if key < target_node.keys[0] {
            utils::remove_with_shift(&mut target_node.data, 0);
            utils::remove_with_shift(&mut target_node.keys, 0);
            target_node.keys_count -= 1;
            target_node.data_count -= 1;
            return ErasePosition::First;
        }
        if key >= target_node.keys[target_node.keys_count - 1] {
            utils::remove_with_shift(&mut target_node.data, target_node.data_count - 1);
            utils::remove_with_shift(&mut target_node.keys, target_node.keys_count - 1);
            target_node.keys_count -= 1;
            target_node.data_count -= 1;
            return ErasePosition::Other;
        }
    } else {
        println!("erase_key_data leaf from={:?} key={}", target_node.id, key);
    }

    for i in 0..target_node.keys_count {
        if target_node.keys[i] == key {
            utils::remove_with_shift(&mut target_node.keys, i);
            if !target_node.is_leaf {
                utils::remove_with_shift(&mut target_node.data, i + 1);
            } else {
                utils::remove_with_shift(&mut target_node.data, i);
            }
            target_node.keys_count -= 1;
            target_node.data_count -= 1;
            break;
        }
    }
    return ErasePosition::Other;
}

fn take_key_from_low(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    low_side_node: &mut Node,
    middle: Option<i32>,
) -> i32 {
    println!(
        "take_key_from_low target={:?} low={:?}",
        target_node.id, low_side_node.id
    );

    let mut min_key = target_node.first_key();
    let result = min_key;
    if !target_node.is_leaf {
        min_key = middle.unwrap();
        println!(" new minKey={}", min_key);
    } else {
        let max_key = low_side_node.keys[low_side_node.keys_count - 1];
        let max_data = low_side_node.data[low_side_node.data_count - 1].clone();

        if !target_node.is_leaf {
            //TODO! move to resize
            let min_data_node = storage.get_node(max_data.into_id()).unwrap();
            min_data_node.borrow_mut().parent = target_node.id;
        }

        utils::insert_to_array(&mut target_node.keys, 0, max_key);
        utils::insert_to_array(&mut target_node.data, 0, max_data);
        low_side_node.keys_count -= 1;
        low_side_node.data_count -= 1;

        target_node.keys_count += 1;
        target_node.data_count += 1;
    }
    return result;
}

fn take_key_from_high(
    target_node: &mut Node,
    high_side_node: &mut Node,
    middle: Option<i32>,
) -> i32 {
    println!(
        "take_key_from_high target={:?} high={:?} minKey={}",
        target_node.id, high_side_node.id, high_side_node.keys[0]
    );

    let mut min_key = high_side_node.keys[0];
    let mut result = min_key;
    let min_data = high_side_node.data[0].clone();
    if !target_node.is_leaf {
        min_key = middle.unwrap();
        println!(" new minKey={}", min_key);
    }
    {
        let mut position = target_node.keys_count;
        target_node.keys[position] = min_key;
        position = target_node.data_count;
        target_node.data[position] = min_data;

        utils::remove_with_shift(&mut high_side_node.keys, 0);
        utils::remove_with_shift(&mut high_side_node.data, 0);

        high_side_node.keys_count -= 1;
        high_side_node.data_count -= 1;

        target_node.keys_count += 1;
        target_node.data_count += 1;
    }
    if target_node.is_leaf {
        result = high_side_node.keys[0];
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
    if !target_node.is_leaf {
        utils::insert_to_array(
            &mut low_side_node.keys,
            low_side_node.keys_count,
            middle.unwrap(),
        );
        low_side_node.keys_count += 1;
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
    }
}

fn move_to_higher(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    high_side_node: &mut Node,
    middle: Option<i32>,
) {
    println!(
        "move_to_higher target={:?} low={:?}",
        target_node.id, high_side_node.id
    );

    //TODO! opt
    if !target_node.is_leaf {
        utils::insert_to_array(&mut high_side_node.keys, 0, middle.unwrap());
        high_side_node.keys_count += 1;
    }
    for i in 0..target_node.keys_count {
        utils::insert_to_array(&mut high_side_node.keys, i, target_node.keys[i]);
    }

    for i in 0..target_node.data_count {
        utils::insert_to_array(&mut high_side_node.data, i, target_node.data[i].clone());

        if !target_node.is_leaf {
            let node = storage.get_node(target_node.data[i].into_id()).unwrap();
            node.borrow_mut().parent = high_side_node.id;
        }
    }

    high_side_node.keys_count += target_node.keys_count;
    high_side_node.data_count += target_node.data_count;
}

fn erase_key(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    key: i32,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    let first_key = target_node.borrow().keys[0];
    {
        let mut target_node_ref = target_node.borrow_mut();
        let _erase_pos = erase_key_data(&mut target_node_ref, key);
        if target_node_ref.data_count >= t {
            //update keys in parent
            if first_key != target_node_ref.keys[0] && target_node_ref.parent.exists() {
                let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                link_to_parent
                    .borrow_mut()
                    .update_key(target_node_ref.id, target_node_ref.first_key());
            }

            return Ok(toproot.unwrap());
        }
    }
    return resize(storage, target_node, t, toproot);
}

fn resize(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    let mut target_node_ref = target_node.borrow_mut();
    if target_node_ref.data_count >= t {
        return Ok(toproot.unwrap());
    }
    if target_node_ref.keys_count == 0 || target_node_ref.data_count == 0 {
        if target_node_ref.parent.is_empty() {
            if target_node_ref.data_count > 0 && !target_node_ref.is_leaf {
                storage.erase_node(&target_node_ref.id);
                let new_root_res = storage.get_node(target_node_ref.data[0].into_id());
                let new_root = new_root_res.unwrap();
                new_root.borrow_mut().parent.clear();
                return Ok(new_root);
            }
            return Ok(target_node.clone());
        } else {
            todo!()
        }
    }

    let mut link_to_low_side_leaf: Option<RcNode> = None;
    let mut link_to_high_side_leaf: Option<RcNode> = None;
    if target_node_ref.left.exists() {
        // from low side
        //TODO! check result;
        let low_side_leaf = storage.get_node(target_node_ref.left).unwrap();
        link_to_low_side_leaf = Some(low_side_leaf.clone());
        let mut low_side_leaf_ref = low_side_leaf.borrow_mut();

        if low_side_leaf_ref.data_count > t {
            let mut middle: Option<i32> = None;
            if !target_node_ref.is_leaf {
                let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                let ref_to_parent = link_to_parent.borrow();
                let rec = ref_to_parent.find_key(target_node_ref.first_key());
                middle = Some(*rec.unwrap());
            }
            take_key_from_low(
                storage,
                &mut target_node_ref,
                &mut low_side_leaf_ref,
                middle,
            );
            if !target_node_ref.is_leaf {
                let taked_id = target_node_ref.data[0].into_id();
                let taked_node = storage.get_node(taked_id).unwrap();
                taked_node.borrow_mut().parent = target_node_ref.id;
            }
            if target_node_ref.parent.exists() {
                //TODO! check result
                let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                link_to_parent
                    .borrow_mut()
                    .update_key(target_node_ref.id, target_node_ref.first_key());
            }

            return Ok(toproot.unwrap());
        }
    } else if target_node_ref.right.exists() {
        // from high side
        //TODO! check result;
        let high_side_leaf = storage.get_node(target_node_ref.right).unwrap();
        link_to_high_side_leaf = Some(high_side_leaf.clone());
        let mut high_side_leaf_ref = high_side_leaf.borrow_mut();

        if high_side_leaf_ref.data_count > t {
            let min_key = high_side_leaf_ref.keys[0];
            let mut middle: Option<i32> = None;
            if !target_node_ref.is_leaf {
                let link_to_parent = storage.get_node(high_side_leaf_ref.parent).unwrap();
                let ref_to_praent = link_to_parent.borrow();
                let rec = ref_to_praent.find_key(min_key);
                middle = Some(*rec.unwrap());
            }

            let new_min_key =
                take_key_from_high(&mut target_node_ref, &mut high_side_leaf_ref, middle);
            if !target_node_ref.is_leaf {
                let taked_id = target_node_ref.data[target_node_ref.data_count - 1].into_id();
                let taked_node = storage.get_node(taked_id).unwrap();
                taked_node.borrow_mut().parent = target_node_ref.id;
            }

            if target_node_ref.parent.exists() {
                //TODO! check result
                let link_to_parent = storage.get_node(high_side_leaf_ref.parent).unwrap();
                link_to_parent
                    .borrow_mut()
                    .update_key(high_side_leaf_ref.id, new_min_key);
            }

            return Ok(toproot.unwrap());
        }
    }

    //try move to brother
    let mut update_parent = false;
    if target_node_ref.left.exists() {
        let low_side_leaf = if link_to_low_side_leaf.is_some() {
            link_to_low_side_leaf.unwrap()
        } else {
            storage.get_node(target_node_ref.left).unwrap()
        };
        let mut low_side_leaf_ref = low_side_leaf.borrow_mut();

        let size_of_low = low_side_leaf_ref.keys_count;
        if (size_of_low + target_node_ref.keys_count) < 2 * t {
            let min_key = target_node_ref.keys[0];
            let mut middle: Option<i32> = None;
            if target_node_ref.parent.exists() {
                let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                if !target_node_ref.is_leaf {
                    let ref_to_parent = link_to_parent.borrow();
                    let rec = ref_to_parent.find_key(min_key);
                    middle = Some(*rec.unwrap());
                }
                link_to_parent.borrow_mut().erase_link(target_node_ref.id);
            }

            move_to_lower(
                storage,
                &mut target_node_ref,
                &mut low_side_leaf_ref,
                middle,
            );

            storage.erase_node(&target_node_ref.id);

            //TODO! check result;
            if target_node_ref.right.exists() {
                let right_side = storage.get_node(target_node_ref.right).unwrap();
                right_side.borrow_mut().left = target_node_ref.left;
            }
            low_side_leaf_ref.right = target_node_ref.right;
            update_parent = true;
        }
    } else if target_node_ref.right.exists() {
        let high_side_leaf = if link_to_high_side_leaf.is_some() {
            link_to_high_side_leaf.unwrap()
        } else {
            storage.get_node(target_node_ref.right).unwrap()
        };

        let mut high_side_leaf_ref = high_side_leaf.borrow_mut();
        let size_of_high = high_side_leaf_ref.keys_count;
        if (size_of_high + target_node_ref.keys_count) < 2 * t {
            let min_key = high_side_leaf_ref.keys[0];
            let mut middle: Option<i32> = None;
            if target_node_ref.parent.exists() {
                let link_to_parent = storage.get_node(high_side_leaf_ref.parent).unwrap();
                if !target_node_ref.is_leaf {
                    let ref_to_parent = link_to_parent.borrow();
                    let rec = ref_to_parent.find_key(min_key);
                    middle = Some(*rec.unwrap());
                }
                link_to_parent.borrow_mut().erase_link(target_node_ref.id);
            }

            move_to_higher(
                storage,
                &mut target_node_ref,
                &mut high_side_leaf_ref,
                middle,
            );

            high_side_leaf_ref.left = target_node_ref.left;
            storage.erase_node(&target_node_ref.id);

            update_parent = true;
        }
    }

    if update_parent && target_node_ref.parent.exists() {
        //TODO! check result
        let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
        if link_to_parent.borrow().keys_count < t {
            return resize(storage, &link_to_parent, t, toproot);
        }
    }

    return Ok(toproot.unwrap());
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

    println!("remove from {:?}", target_node.borrow().id);
    return erase_key(storage, &target_node, key, t, Some(root.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::insert::insert;
    use crate::mocks::MockNodeStorage;
    use crate::node::Node;
    use crate::read::{find, map, map_rev};
    use crate::rec::Record;

    fn make_tree(nodes_count: usize, t: usize) -> (MockNodeStorage, RcNode, Vec<i32>) {
        let mut keys = Vec::with_capacity(t * 2);
        let mut recs = Vec::with_capacity(t * 2);
        for _i in 0..(t * 2) {
            recs.push(Record::from_i32(0));
            keys.push(0i32);
        }
        let mut root_node = Node::new_leaf(types::Id(1), keys, recs, 0, 0);

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

    #[test]
    fn many_inserts() {
        let t = 3;
        for hight in 3..75 {
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
                        print_state(&str_before, &str_after);
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
                        print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_ok());
                    let d = find_res.unwrap();
                    if d.into_i32() != k {
                        print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), k);
                }
            }
        }

        //TODO check map map_rev
    }

    #[test]
    fn many_inserts_rev() {
        let t = 3;
        for hight in 3..75 {
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
                //println!("remove {:?}", i);
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
                        print_state(&str_before, &str_after);
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
                        print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_ok());
                    assert_eq!(find_res.unwrap().into_i32(), k);
                }
            }
        }

        //TODO check map map_rev
    }

    fn print_state(str_before: &String, str_after: &String) {
        print!("digraph G {{");
        print!("{}", str_before);
        print!("{}", str_after);
        println!("}}");
    }
}
