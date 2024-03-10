use crate::{node::RcNode, nodestorage::NodeStorage, read, rm::erase_key, Result};

pub fn remove_key<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: i32,
) -> Result<RcNode> {
    let target_node: RcNode;

    let scan_result = read::scan(storage, &root, key);
    if scan_result.is_err() {
        return scan_result;
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
    return erase_key(storage, &target_node, key, Some(root.clone()));
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::prelude::*;

    pub fn make_tree(nodes_count: usize, t: usize) -> (MockNodeStorage, RcNode, Vec<i32>) {
        let mut root_node = Node::new_leaf_with_size(Id(1), t);

        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(t));
        storage.add_node(&root_node);

        let mut key: i32 = 1;
        let mut keys = Vec::new();
        while storage.size() <= nodes_count {
            key += 1;
            let res = insert(&mut storage, &root_node, key, &Record::from_i32(key));
            keys.push(key);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                let v = res.unwrap().unwrap();
                assert_eq!(v.into_i32(), i);
            }
        }
        return (storage, root_node, keys);
    }

    #[test]
    fn remove_from_leaf() -> Result<()> {
        let leaf = Node::new_leaf(
            Id(1),
            vec![1, 2, 3, 4, 5, 6],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(3),
                Record::from_i32(4),
                Record::from_i32(5),
                Record::from_i32(6),
            ],
            6,
            6,
        );
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        storage.add_node(&leaf);

        let result = crate::rm::erase_key(&mut storage, &leaf, 2, Some(leaf.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![1, 3, 4, 5, 6, 2]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_i32(1),
                    Record::from_i32(3),
                    Record::from_i32(4),
                    Record::from_i32(5),
                    Record::from_i32(6),
                    Record::from_i32(2),
                ]
            );
            assert_eq!(ref_root.keys_count, 5);
            assert_eq!(ref_root.data_count, 5);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_update_parent() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        let leaf1 = Node::new_leaf(
            Id(1),
            vec![1, 2, 3, 4],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(3),
                Record::from_i32(4),
            ],
            4,
            4,
        );
        storage.add_node(&leaf1);

        let leaf2 = Node::new_leaf(
            Id(2),
            vec![5, 6, 7, 8],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(7),
                Record::from_i32(8),
            ],
            4,
            4,
        );
        storage.add_node(&leaf2);

        let root = Node::new_root(
            Id(3),
            vec![5, 0, 0, 0],
            vec![
                Record::from_id(Id(1)),
                Record::from_id(Id(2)),
                Record::from_id(Id::empty()),
                Record::from_id(Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);
        leaf1.borrow_mut().parent = root.borrow().id;
        leaf2.borrow_mut().parent = root.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf2, 5, Some(root.clone()));
        assert!(result.is_ok());
        {
            let newroot = result.unwrap();
            let ref_root = newroot.borrow();
            assert_eq!(ref_root.id, root.borrow().id);
            assert_eq!(ref_root.keys, vec![6, 0, 0, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_id(Id(1)),
                    Record::from_id(Id(2)),
                    Record::from_id(Id::empty()),
                    Record::from_id(Id::empty()),
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
                    Record::from_i32(6),
                    Record::from_i32(7),
                    Record::from_i32(8),
                    Record::from_i32(5),
                ]
            );
            assert_eq!(ref_leaf2.keys_count, 3);
            assert_eq!(ref_leaf2.data_count, 3);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_take_from_lower() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));

        let root = Node::new_root(
            Id(3),
            vec![5, 0, 0, 0],
            vec![
                Record::from_id(Id(1)),
                Record::from_id(Id(2)),
                Record::from_id(Id::empty()),
                Record::from_id(Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);

        let leaf_high = Node::new_leaf(
            Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(7),
                Record::from_i32(0),
            ],
            3,
            3,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 3, 4],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(3),
                Record::from_i32(4),
            ],
            4,
            4,
        );
        storage.add_node(&leaf_low);

        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf_high, 6, Some(root.clone()));
        assert!(result.is_ok());

        {
            let ref_node: std::cell::RefMut<'_, Node> = root.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 0, 0, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_id(Id(1)),
                    Record::from_id(Id(2)),
                    Record::from_id(Id::empty()),
                    Record::from_id(Id::empty()),
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
                    Record::from_i32(4),
                    Record::from_i32(5),
                    Record::from_i32(7),
                    Record::from_i32(0),
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
                    Record::from_i32(1),
                    Record::from_i32(2),
                    Record::from_i32(3),
                    Record::from_i32(4),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_take_from_high() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        let root = Node::new_root(
            Id(3),
            vec![9, 0, 0, 0],
            vec![
                Record::from_id(Id(1)),
                Record::from_id(Id(2)),
                Record::from_id(Id::empty()),
                Record::from_id(Id::empty()),
            ],
            1,
            2,
        );
        storage.add_node(&root);

        let leaf_low = Node::new_leaf(
            Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(7),
                Record::from_i32(0),
            ],
            3,
            3,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            Id(2),
            vec![9, 10, 11, 12],
            vec![
                Record::from_i32(9),
                Record::from_i32(10),
                Record::from_i32(11),
                Record::from_i32(12),
            ],
            4,
            4,
        );
        storage.add_node(&leaf_high);
        leaf_low.borrow_mut().right = leaf_high.borrow().id;
        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf_low, 6, Some(root.clone()));
        assert!(result.is_ok());

        {
            let newroot = result.unwrap();
            let ref_root = newroot.borrow();
            assert_eq!(ref_root.id, root.borrow().id);
            assert_eq!(ref_root.keys, vec![10, 0, 0, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_id(Id(1)),
                    Record::from_id(Id(2)),
                    Record::from_id(Id::empty()),
                    Record::from_id(Id::empty()),
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
                    Record::from_i32(5),
                    Record::from_i32(7),
                    Record::from_i32(9),
                    Record::from_i32(6),
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
                    Record::from_i32(10),
                    Record::from_i32(11),
                    Record::from_i32(12),
                    Record::from_i32(9),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_move_to_lower() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));

        let leaf_high = Node::new_leaf(
            Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_low);
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf_high, 6, Some(leaf_high.clone()));
        assert!(result.is_ok());

        assert!(!storage.is_exists(leaf_high.borrow().id));
        {
            let ref_node = leaf_low.borrow_mut();
            assert_eq!(ref_node.keys, vec![1, 2, 5, 0]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_i32(1),
                    Record::from_i32(2),
                    Record::from_i32(5),
                    Record::from_i32(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_move_to_high() -> Result<()> {
        let leaf_low = Node::new_leaf(
            Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(7),
                Record::from_i32(0),
            ],
            3,
            3,
        );
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            Id(2),
            vec![9, 10, 0, 0],
            vec![
                Record::from_i32(9),
                Record::from_i32(10),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_high);
        leaf_low.borrow_mut().right = leaf_high.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf_low, 6, Some(leaf_low.clone()));
        assert!(result.is_ok());

        assert!(!storage.is_exists(leaf_low.borrow().id));
        {
            let ref_node = leaf_high.borrow_mut();
            assert_eq!(ref_node.keys, vec![5, 7, 9, 10]);
            assert_eq!(
                ref_node.data,
                vec![
                    Record::from_i32(5),
                    Record::from_i32(7),
                    Record::from_i32(9),
                    Record::from_i32(10),
                ]
            );
            assert_eq!(ref_node.keys_count, 4);
            assert_eq!(ref_node.data_count, 4);
        }
        return Ok(());
    }

    #[test]
    #[should_panic]
    fn remove_from_node_first() {
        let node = Node::new_root(
            Id(1),
            vec![5, 8, 0],
            vec![
                Record::from_i32(1),
                Record::from_i32(5),
                Record::from_i32(10),
            ],
            2,
            3,
        );
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        storage.add_node(&node);

        let result = crate::rm::erase_key(&mut storage, &node, 5, Some(node.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![8, 0, 5]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_i32(1),
                    Record::from_i32(10),
                    Record::from_i32(5),
                ]
            );
            assert_eq!(ref_root.keys_count, 1);
            assert_eq!(ref_root.data_count, 2);
        }
    }

    #[test]
    fn remove_from_leaf_move_to_lower_update_parent() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));

        let root = Node::new_root(
            Id(4),
            vec![5, 12, 0, 0],
            vec![
                Record::from_id(Id(2)),
                Record::from_id(Id(1)),
                Record::from_id(Id(3)),
                Record::from_id(Id::empty()),
            ],
            2,
            3,
        );
        storage.add_node(&root);

        let leaf_extra = Node::new_leaf(
            Id(3),
            vec![12, 15, 0, 0],
            vec![
                Record::from_i32(12),
                Record::from_i32(15),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_high = Node::new_leaf(
            Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(0),
                Record::from_i32(0),
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
        let result = crate::rm::erase_key(&mut storage, &leaf_high, 6, Some(root.clone()));
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
                    Record::from_i32(1),
                    Record::from_i32(2),
                    Record::from_i32(5),
                    Record::from_i32(0),
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
                    Record::from_id(Id(2)),
                    Record::from_id(Id(3)),
                    Record::from_id(Id::empty()),
                    Record::from_id(Id(1)),
                ]
            );
            assert_eq!(ref_node.keys_count, 1);
            assert_eq!(ref_node.data_count, 2);
        }
        return Ok(());
    }

    #[test]
    fn remove_from_leaf_move_to_high_update_parent() -> Result<()> {
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(2));
        /*
              9            15
         5 6    9 10, 0, 0   15 16
        */
        let root = Node::new_root(
            Id(3),
            vec![9, 15, 0, 0],
            vec![
                Record::from_id(Id(1)),
                Record::from_id(Id(2)),
                Record::from_id(Id(4)),
                Record::from_id(Id::empty()),
            ],
            2,
            3,
        );
        storage.add_node(&root);

        let leaf_extra = Node::new_leaf(
            Id(4),
            vec![15, 16, 0, 0],
            vec![
                Record::from_i32(15),
                Record::from_i32(16),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_low = Node::new_leaf(
            Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_i32(5),
                Record::from_i32(6),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            Id(2),
            vec![9, 10, 0, 0],
            vec![
                Record::from_i32(9),
                Record::from_i32(10),
                Record::from_i32(0),
                Record::from_i32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_high);

        leaf_low.borrow_mut().right = leaf_high.borrow().id;
        leaf_high.borrow_mut().parent = root.borrow().id;
        leaf_low.borrow_mut().parent = root.borrow().id;
        leaf_extra.borrow_mut().parent = root.borrow().id;

        let result = crate::rm::erase_key(&mut storage, &leaf_low, 6, Some(root.clone()));
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
                    Record::from_id(Id(2)),
                    Record::from_id(Id(4)),
                    Record::from_id(Id::empty()),
                    Record::from_id(Id(1)),
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
                    Record::from_i32(5),
                    Record::from_i32(9),
                    Record::from_i32(10),
                    Record::from_i32(0),
                ]
            );
            assert_eq!(ref_node.keys_count, 3);
            assert_eq!(ref_node.data_count, 3);
        }
        return Ok(());
    }

    fn many_inserts(t: usize, maxnodes: usize) -> Result<()> {
        for hight in 3..maxnodes {
            // let hight = 22;
            let (mut storage, mut root_node, keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i)?;
                assert!(res.is_some());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            for i in 2..=key {
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_some());
                assert_eq!(find_res.unwrap().into_i32(), i);
                // /                println!("remove {:?}", i);

                let str_before = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("before"),
                );

                let remove_res = crate::remove::remove_key(&mut storage, &root_node, i);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();

                let str_after = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("after"),
                );

                let mut mapped_values = Vec::new();
                map(&mut storage, &root_node, i, key, &mut |k, v| {
                    assert_eq!(v.into_i32(), k);
                    mapped_values.push(k);
                })
                .unwrap();

                for i in 1..mapped_values.len() {
                    if mapped_values[i - 1] >= mapped_values[i] {
                        println!("bad order");
                        debug::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    assert!(i == key);
                    break;
                }
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_none());

                // print_state(&str_before, &str_after);
                // break;
                for k in (i + 1)..key {
                    //println!("? {:?}", k);
                    // if k == 14 {
                    //     println!("!!");
                    // }
                    let find_res = find(&mut storage, &root_node, k)?;
                    if find_res.is_none() {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_some());
                    let d = find_res.unwrap();
                    if d.into_i32() != k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), k);
                }
            }
        }
        return Ok(());
    }

    fn many_inserts_rev(t: usize, maxnodes: usize) -> Result<()> {
        for hight in 3..maxnodes {
            let (mut storage, mut root_node, keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i)?;
                assert!(res.is_some());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            for i in (2..=key).rev() {
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_some());
                assert_eq!(find_res.unwrap().into_i32(), i);
                println!(">> remove {:?}", i);
                let str_before = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("before"),
                );

                let remove_res = remove_key(&mut storage, &root_node, i);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();
                let str_after = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("after"),
                );

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
                        debug::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    assert!(i == key);
                    break;
                }
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_none());
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
                    let find_res = find(&mut storage, &root_node, k)?;
                    if find_res.is_none() {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_some());
                    let d = find_res.unwrap();
                    if d.into_i32() != k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), k);
                }
            }
        }
        return Ok(());
    }

    fn many_inserts_middle_range(t: usize, maxnodes: usize) -> Result<()> {
        for hight in 3..maxnodes {
            //    let hight = 21;
            let (mut storage, mut root_node, mut keys) = make_tree(hight, t);

            let key = *keys.last().unwrap();
            for i in 2..=key {
                let res = find(&mut storage, &root_node, i)?;
                assert!(res.is_some());
                assert_eq!(res.unwrap().into_i32(), i);
            }

            /*let first = &keys[0..keys.len() / 2];
            let last = &keys[keys.len() / 2..];
            let new_key_list = [last, first].concat();

            for i in new_key_list */

            while keys.len() > 0 {
                let i = keys[keys.len() / 2];
                keys.remove(keys.len() / 2);
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_some());
                assert_eq!(find_res.unwrap().into_i32(), i);
                println!(">> {} {} remove {:?} size: {}", hight, t, i, storage.size());
                // if i == 29 {
                //     println!("!");
                // }
                let str_before = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("before"),
                );

                let remove_res = remove_key(&mut storage, &root_node, i);
                assert!(remove_res.is_ok());
                root_node = remove_res.unwrap();

                let str_after = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("after"),
                );
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
                        debug::print_state(&str_before, &str_after);
                        assert!(mapped_values[i - 1] < mapped_values[i]);
                    }
                }

                if root_node.borrow().is_empty() {
                    break;
                }
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_none());
                // assert!(!find_res.is_err());
                // print_state(&str_before, &str_after);
                // break;
                for k in &keys {
                    // println!("? {:?}", k);
                    // if *k == 20 {
                    //     println!("!!");
                    // }
                    let find_res = find(&mut storage, &root_node, *k)?;
                    if find_res.is_none() {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert!(find_res.is_some());
                    let d = find_res.unwrap();
                    if d.into_i32() != *k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_i32(), *k);
                }
            }
        }
        return Ok(());
    }

    #[test]
    fn many_inserts_3_22() -> Result<()> {
        many_inserts(3, 22)
    }

    #[test]
    fn many_inserts_7_22() -> Result<()> {
        many_inserts(7, 22)
    }

    #[test]
    fn many_inserts_16_10() -> Result<()> {
        many_inserts(16, 22)
    }

    #[test]
    fn many_inserts_rev_3_22() -> Result<()> {
        many_inserts_rev(3, 22)
    }

    #[test]
    fn many_inserts_rev_7_22() -> Result<()> {
        many_inserts_rev(7, 22)
    }

    #[test]
    fn many_inserts_rev_16_22() -> Result<()> {
        many_inserts_rev(16, 22)
    }

    #[test]
    fn many_inserts_middle_range_3_22() -> Result<()> {
        many_inserts_middle_range(3, 22)
    }

    #[test]
    fn many_inserts_middle_range_7_22() -> Result<()> {
        many_inserts_middle_range(7, 22)
    }

    #[test]
    fn remove_from_middle_leaf() -> Result<()> {
        let (mut storage, mut root_node, _keys) = make_tree(7, 3);

        let res = insert(&mut storage, &root_node, 1, &Record::from_i32(1));
        root_node = res.unwrap();

        let str_before = crate::prelude::debug::storage_to_string(
            &storage,
            root_node.clone(),
            true,
            &String::from("before"),
        );

        let remove_res = remove_key(&mut storage, &root_node, 5);
        assert!(remove_res.is_ok());
        root_node = remove_res.unwrap();

        let str_after =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

        // {
        //     print_state(&str_before, &str_after);
        // }

        for i in 1..19 {
            if i == 5 {
                continue;
            }
            let find_res = find(&mut storage, &root_node, i);
            if find_res.is_err() {
                debug::print_state(&str_before, &str_after);
            }
            assert!(find_res.is_ok());
            assert_eq!(find_res.unwrap().unwrap().into_i32(), i);
        }
        return Ok(());
    }
}
