use crate::{node::RcNode, nodestorage::NodeStorage, types, utils};

pub fn erase_key(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    key: i32,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    if target_node.borrow().is_leaf {
        let mut target_node_ref = target_node.borrow_mut();
        for i in 0..target_node_ref.keys_count {
            if target_node_ref.keys[i] == key {
                utils::remove_with_shift(&mut target_node_ref.keys, i);
                utils::remove_with_shift(&mut target_node_ref.data, i);
                target_node_ref.keys_count -= 1;
                target_node_ref.data_count -= 1;
                break;
            }
        }
        if target_node_ref.data_count < t {
            let mut try_move_to_brother = true;
            let mut size_of_low = 2 * t;
            let mut size_of_high = 2 * t;

            let mut removed_key_from_parent = 0i32;
            if target_node_ref.left != types::EMPTY_ID {
                //TODO! check result;
                let low_side_leaf = storage.get_node(target_node_ref.left).unwrap();
                let mut low_side_leaf_ref = low_side_leaf.borrow_mut();
                size_of_low = low_side_leaf_ref.keys_count;
                if low_side_leaf_ref.data_count > t {
                    let max_key = low_side_leaf_ref.keys[low_side_leaf_ref.keys_count - 1];
                    let max_data = low_side_leaf_ref.data[low_side_leaf_ref.data_count - 1].clone();

                    utils::insert_to_array(&mut target_node_ref.keys, 0, max_key);
                    utils::insert_to_array(&mut target_node_ref.data, 0, max_data);
                    low_side_leaf_ref.keys_count -= 1;
                    low_side_leaf_ref.data_count -= 1;

                    target_node_ref.keys_count += 1;
                    target_node_ref.data_count += 1;
                    try_move_to_brother = false;
                    removed_key_from_parent = max_key;
                }
            }

            if target_node_ref.right != types::EMPTY_ID {
                //TODO! check result;
                let high_side_leaf = storage.get_node(target_node_ref.right).unwrap();
                let mut high_side_leaf_ref = high_side_leaf.borrow_mut();
                size_of_high = high_side_leaf_ref.keys_count;
                if high_side_leaf_ref.data_count > t {
                    let min_key = high_side_leaf_ref.keys[0];
                    let min_data = high_side_leaf_ref.data[0].clone();

                    let mut position = target_node_ref.keys_count;
                    target_node_ref.keys[position] = min_key;
                    position = target_node_ref.data_count;
                    target_node_ref.data[position] = min_data;

                    utils::remove_with_shift(&mut high_side_leaf_ref.keys, 0);
                    utils::remove_with_shift(&mut high_side_leaf_ref.data, 0);

                    high_side_leaf_ref.keys_count -= 1;
                    high_side_leaf_ref.data_count -= 1;

                    target_node_ref.keys_count += 1;
                    target_node_ref.data_count += 1;
                    try_move_to_brother = false;
                    removed_key_from_parent = min_key;
                }
            }

            if try_move_to_brother {
                if (size_of_low + target_node_ref.keys_count) < 2 * t {
                    let low_side_leaf = storage.get_node(target_node_ref.left).unwrap();
                    let mut low_side_leaf_ref = low_side_leaf.borrow_mut();

                    let low_keys_count = low_side_leaf_ref.keys_count;
                    for i in 0..target_node_ref.keys_count {
                        low_side_leaf_ref.keys[low_keys_count + i] = target_node_ref.keys[i];
                    }

                    let low_data_count = low_side_leaf_ref.data_count;
                    for i in 0..target_node_ref.data_count {
                        low_side_leaf_ref.data[low_data_count + i] =
                            target_node_ref.data[i].clone();
                    }

                    low_side_leaf_ref.keys_count += target_node_ref.keys_count;
                    low_side_leaf_ref.data_count += target_node_ref.data_count;

                    storage.erase_node(&target_node_ref.id);
                    //TODO! update parent. with test
                } else if (size_of_high + target_node_ref.keys_count) < 2 * t {
                    let high_side_leaf = storage.get_node(target_node_ref.right).unwrap();
                    let mut high_side_leaf_ref = high_side_leaf.borrow_mut();

                    //TODO! opt

                    for i in 0..target_node_ref.keys_count {
                        utils::insert_to_array(
                            &mut high_side_leaf_ref.keys,
                            i,
                            target_node_ref.keys[i],
                        );
                    }

                    for i in 0..target_node_ref.data_count {
                        utils::insert_to_array(
                            &mut high_side_leaf_ref.data,
                            i,
                            target_node_ref.data[i].clone(),
                        );
                    }

                    high_side_leaf_ref.keys_count += target_node_ref.keys_count;
                    high_side_leaf_ref.data_count += target_node_ref.data_count;

                    storage.erase_node(&target_node_ref.id);
                    //TODO! update parent. with test
                }

                //TODO! update parent. with test
            }
            return Ok(toproot.unwrap());
        } else {
            return Ok(toproot.unwrap());
        }
    } else {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::node::Node;
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
    fn remove_from_leaf_take_from_lower() {
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_low);
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = erase_key(&mut storage, &leaf_high, 6, 3, Some(leaf_high.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![4, 5, 7, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_u8(4),
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_root.keys_count, 3);
            assert_eq!(ref_root.data_count, 3);
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
    fn remove_from_leaf_move_to_lower() {
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
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
    fn remove_from_leaf_take_from_high() {
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_high);
        leaf_low.borrow_mut().right = leaf_high.borrow().id;

        let result = erase_key(&mut storage, &leaf_low, 6, 3, Some(leaf_low.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![5, 7, 9, 6]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(9),
                    Record::from_u8(6),
                ]
            );
            assert_eq!(ref_root.keys_count, 3);
            assert_eq!(ref_root.data_count, 3);
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
}
