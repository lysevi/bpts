use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    read, types, utils,
};

fn erase_key_data(target_node: &mut Node, key: i32) {
    let is_leaf = target_node.is_leaf;

    if !is_leaf {
        if key < target_node.keys[0] {
            utils::remove_with_shift(&mut target_node.data, 0);
            utils::remove_with_shift(&mut target_node.keys, 0);
            target_node.keys_count -= 1;
            target_node.data_count -= 1;
            return;
        }
        if key > target_node.keys[target_node.keys_count - 1] {
            utils::remove_with_shift(&mut target_node.data, target_node.data_count - 1);
            utils::remove_with_shift(&mut target_node.keys, target_node.keys_count - 1);
            target_node.keys_count -= 1;
            target_node.data_count -= 1;
            return;
        }
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
}

fn take_key_from_low(target_node: &mut Node, low_side_node: &mut Node) {
    if !target_node.is_leaf {
        todo!();
    } else {
        let max_key = low_side_node.keys[low_side_node.keys_count - 1];
        let max_data = low_side_node.data[low_side_node.data_count - 1].clone();

        utils::insert_to_array(&mut target_node.keys, 0, max_key);
        utils::insert_to_array(&mut target_node.data, 0, max_data);
        low_side_node.keys_count -= 1;
        low_side_node.data_count -= 1;

        target_node.keys_count += 1;
        target_node.data_count += 1;
    }
}

fn take_key_from_high(target_node: &mut Node, high_side_node: &mut Node) {
    if !target_node.is_leaf {
        println!("! take_key_from_high node");
    }
    {
        let min_key = high_side_node.keys[0];
        let min_data = high_side_node.data[0].clone();

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
}

fn move_to_lower(target_node: &mut Node, low_side_node: &mut Node) {
    if !target_node.is_leaf {
        todo!();
    } else {
        let low_keys_count = low_side_node.keys_count;
        for i in 0..target_node.keys_count {
            low_side_node.keys[low_keys_count + i] = target_node.keys[i];
        }

        let low_data_count = low_side_node.data_count;
        for i in 0..target_node.data_count {
            low_side_node.data[low_data_count + i] = target_node.data[i].clone();
        }

        low_side_node.keys_count += target_node.keys_count;
        low_side_node.data_count += target_node.data_count;
    }
}

fn move_to_higher(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    high_side_node: &mut Node,
) {
    //TODO! opt

    if !target_node.is_leaf {
        //TODO! check
        let first_leaf = storage.get_node(high_side_node.data[0].into_id()).unwrap();
        let first_key = first_leaf.borrow().keys[0];
        utils::insert_to_array(&mut high_side_node.keys, 0, first_key);
    }
    for i in 0..target_node.keys_count {
        utils::insert_to_array(&mut high_side_node.keys, i, target_node.keys[i]);
    }

    for i in 0..target_node.data_count {
        utils::insert_to_array(&mut high_side_node.data, i, target_node.data[i].clone());
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

    let mut target_node_ref = target_node.borrow_mut();
    erase_key_data(&mut target_node_ref, key);

    if target_node_ref.keys_count == 0 || target_node_ref.data_count == 0 {
        if target_node_ref.parent == types::EMPTY_ID {
            if target_node_ref.data_count > 0 && !target_node_ref.is_leaf {
                storage.erase_node(&target_node_ref.id);
                let new_root = storage.get_node(target_node_ref.data[0].into_id());
                return Ok(new_root.unwrap());
            }
            //TODO add test
            //TODO! check result
            let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
            storage.erase_node(&target_node_ref.id);
            return erase_key(
                storage,
                &link_to_parent,
                target_node_ref.first_key(),
                t,
                toproot,
            );
        } else {
            todo!()
        }
    }
    if target_node_ref.data_count >= t {
        //update keys in parent
        if first_key != target_node_ref.keys[0] && target_node_ref.parent != types::EMPTY_ID {
            let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
            link_to_parent
                .borrow_mut()
                .update_key(first_key, target_node_ref.first_key());
        }

        return Ok(toproot.unwrap());
    } else {
        let mut link_to_low_side_leaf: Option<RcNode> = None;
        let mut link_to_high_side_leaf: Option<RcNode> = None;
        if target_node_ref.left != types::EMPTY_ID {
            // from low side
            //TODO! check result;
            let low_side_leaf = storage.get_node(target_node_ref.left).unwrap();
            link_to_low_side_leaf = Some(low_side_leaf.clone());
            let mut low_side_leaf_ref = low_side_leaf.borrow_mut();

            if low_side_leaf_ref.data_count > t {
                take_key_from_low(&mut target_node_ref, &mut low_side_leaf_ref);

                if target_node_ref.parent != types::EMPTY_ID {
                    //TODO! check result
                    let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                    link_to_parent
                        .borrow_mut()
                        .update_key(first_key, target_node_ref.first_key());
                }

                return Ok(toproot.unwrap());
            }
        }
        if target_node_ref.right != types::EMPTY_ID {
            // from high side
            //TODO! check result;
            let high_side_leaf = storage.get_node(target_node_ref.right).unwrap();
            link_to_high_side_leaf = Some(high_side_leaf.clone());
            let mut high_side_leaf_ref = high_side_leaf.borrow_mut();

            if high_side_leaf_ref.data_count > t {
                let min_key = high_side_leaf_ref.keys[0];
                take_key_from_high(&mut target_node_ref, &mut high_side_leaf_ref);

                if target_node_ref.parent != types::EMPTY_ID {
                    //TODO! check result
                    let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
                    link_to_parent
                        .borrow_mut()
                        .update_key(min_key, high_side_leaf_ref.first_key());
                }

                return Ok(toproot.unwrap());
            }
        }

        //try move to brother
        let mut update_parent = false;
        if target_node_ref.left != types::EMPTY_ID {
            let low_side_leaf = if link_to_low_side_leaf.is_some() {
                link_to_low_side_leaf.unwrap()
            } else {
                storage.get_node(target_node_ref.left).unwrap()
            };
            let mut low_side_leaf_ref = low_side_leaf.borrow_mut();

            let size_of_low = low_side_leaf_ref.keys_count;
            if (size_of_low + target_node_ref.keys_count) < 2 * t {
                move_to_lower(&mut target_node_ref, &mut low_side_leaf_ref);

                storage.erase_node(&target_node_ref.id);

                //TODO! check result;
                if target_node_ref.right != types::EMPTY_ID {
                    let right_side = storage.get_node(target_node_ref.right).unwrap();
                    right_side.borrow_mut().left = target_node_ref.left;
                }
                low_side_leaf_ref.right = target_node_ref.right;
                update_parent = true;
            }
        }

        if target_node_ref.right != types::EMPTY_ID {
            let high_side_leaf = if link_to_high_side_leaf.is_some() {
                link_to_high_side_leaf.unwrap()
            } else {
                storage.get_node(target_node_ref.right).unwrap()
            };

            let mut high_side_leaf_ref = high_side_leaf.borrow_mut();
            let size_of_high = high_side_leaf_ref.keys_count;
            if (size_of_high + target_node_ref.keys_count) < 2 * t {
                move_to_higher(storage, &mut target_node_ref, &mut high_side_leaf_ref);

                high_side_leaf_ref.left = target_node_ref.left;
                storage.erase_node(&target_node_ref.id);
                update_parent = true;
            }
        }

        if update_parent && target_node_ref.parent != types::EMPTY_ID {
            //TODO! check result
            let link_to_parent = storage.get_node(target_node_ref.parent).unwrap();
            return erase_key(
                storage,
                &link_to_parent,
                target_node_ref.first_key(),
                t,
                toproot,
            );
        }

        return Ok(toproot.unwrap());
    }
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
                Record::from_id(types::EMPTY_ID),
                Record::from_id(types::EMPTY_ID),
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
                    Record::from_id(types::EMPTY_ID),
                    Record::from_id(types::EMPTY_ID),
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
                Record::from_id(types::EMPTY_ID),
                Record::from_id(types::EMPTY_ID),
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
            assert_eq!(ref_node.keys, vec![4, 0, 0, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_id(types::Id(1)),
                    Record::from_id(types::Id(2)),
                    Record::from_id(types::EMPTY_ID),
                    Record::from_id(types::EMPTY_ID),
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
                Record::from_id(types::EMPTY_ID),
                Record::from_id(types::EMPTY_ID),
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
                    Record::from_id(types::EMPTY_ID),
                    Record::from_id(types::EMPTY_ID),
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
                Record::from_id(types::EMPTY_ID),
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
                    Record::from_id(types::EMPTY_ID),
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
                Record::from_id(types::EMPTY_ID),
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
                    Record::from_id(types::EMPTY_ID),
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
        let mut root_node = Node::new_leaf(
            types::Id(1),
            vec![0, 0, 0, 0, 0, 0],
            vec![
                Record::from_i32(0),
                Record::from_i32(0),
                Record::from_i32(0),
                Record::from_i32(0),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            0,
            0,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let mut key: i32 = 1;
        while storage.size() < 10 {
            key += 1;
            println!("+ {:?} root:{:?}", key, root_node.borrow().id);
            if key == 22 {
                println!("kv 22");
            }
            let res = insert(&mut storage, &root_node, key, &Record::from_i32(key), 3);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                //println!("! {:?}", i);

                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }
        }

        for i in 2..key {
            let find_res = find(&mut storage, &root_node, i);
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().into_i32(), i);
            println!("remove {:?}", i);
            if i == 3 {
                println!("!");
            }
            let remove_res = remove_key(&mut storage, &root_node, i, 3);
            assert!(remove_res.is_ok());

            root_node = remove_res.unwrap();
            let find_res = find(&mut storage, &root_node, i);
            assert!(!find_res.is_err());
        }

        //TODO check map map_rev
    }
}
