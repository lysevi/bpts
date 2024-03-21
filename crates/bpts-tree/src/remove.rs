use crate::{node::RcNode, nodestorage::NodeStorage, read, record::Record, rm::erase_key, Result};

pub fn remove_key_with_data<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: u32,
) -> Result<(Record, RcNode)> {
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
    let res = target_node.borrow().find(storage.get_cmp(), key);
    if res.is_none() {
        println!("!");
    }

    let new_root = erase_key(storage, &target_node, key, Some(root.clone()))?;
    return Ok((res.unwrap(), new_root));
}

pub fn remove_key<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: u32,
) -> Result<RcNode> {
    let subres = remove_key_with_data(storage, root, key);
    match subres {
        Ok(v) => Ok(v.1),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use std::collections::HashSet;

    use crate::{prelude::*, types};

    pub fn make_tree(nodes_count: usize, t: usize) -> (MockNodeStorage, RcNode, Vec<u32>) {
        let mut root_node = Node::new_leaf_with_size(Id(1), t);

        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(t));
        storage.add_node(&root_node);

        let mut key: u32 = 1;
        let mut keys = Vec::new();
        while storage.size() <= nodes_count {
            key += 1;
            let res = insert(&mut storage, &root_node, key, &Record::from_u32(key));
            keys.push(key);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                let v = res.unwrap().unwrap();
                assert_eq!(v.into_u32(), i);
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
                Record::from_u32(1),
                Record::from_u32(2),
                Record::from_u32(3),
                Record::from_u32(4),
                Record::from_u32(5),
                Record::from_u32(6),
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
                    Record::from_u32(1),
                    Record::from_u32(3),
                    Record::from_u32(4),
                    Record::from_u32(5),
                    Record::from_u32(6),
                    Record::from_u32(2),
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
                Record::from_u32(1),
                Record::from_u32(2),
                Record::from_u32(3),
                Record::from_u32(4),
            ],
            4,
            4,
        );
        storage.add_node(&leaf1);

        let leaf2 = Node::new_leaf(
            Id(2),
            vec![5, 6, 7, 8],
            vec![
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(7),
                Record::from_u32(8),
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
                    Record::from_u32(6),
                    Record::from_u32(7),
                    Record::from_u32(8),
                    Record::from_u32(5),
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
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(7),
                Record::from_u32(0),
            ],
            3,
            3,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 3, 4],
            vec![
                Record::from_u32(1),
                Record::from_u32(2),
                Record::from_u32(3),
                Record::from_u32(4),
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
                    Record::from_u32(4),
                    Record::from_u32(5),
                    Record::from_u32(7),
                    Record::from_u32(0),
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
                    Record::from_u32(1),
                    Record::from_u32(2),
                    Record::from_u32(3),
                    Record::from_u32(4),
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
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(7),
                Record::from_u32(0),
            ],
            3,
            3,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            Id(2),
            vec![9, 10, 11, 12],
            vec![
                Record::from_u32(9),
                Record::from_u32(10),
                Record::from_u32(11),
                Record::from_u32(12),
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
                    Record::from_u32(5),
                    Record::from_u32(7),
                    Record::from_u32(9),
                    Record::from_u32(6),
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
                    Record::from_u32(10),
                    Record::from_u32(11),
                    Record::from_u32(12),
                    Record::from_u32(9),
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
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_u32(1),
                Record::from_u32(2),
                Record::from_u32(0),
                Record::from_u32(0),
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
                    Record::from_u32(1),
                    Record::from_u32(2),
                    Record::from_u32(5),
                    Record::from_u32(0),
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
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(7),
                Record::from_u32(0),
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
                Record::from_u32(9),
                Record::from_u32(10),
                Record::from_u32(0),
                Record::from_u32(0),
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
                    Record::from_u32(5),
                    Record::from_u32(7),
                    Record::from_u32(9),
                    Record::from_u32(10),
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
                Record::from_u32(1),
                Record::from_u32(5),
                Record::from_u32(10),
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
                    Record::from_u32(1),
                    Record::from_u32(10),
                    Record::from_u32(5),
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
                Record::from_u32(12),
                Record::from_u32(15),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_high = Node::new_leaf(
            Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );

        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            Id(2),
            vec![1, 2, 0, 0],
            vec![
                Record::from_u32(1),
                Record::from_u32(2),
                Record::from_u32(0),
                Record::from_u32(0),
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
                    Record::from_u32(1),
                    Record::from_u32(2),
                    Record::from_u32(5),
                    Record::from_u32(0),
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
                Record::from_u32(15),
                Record::from_u32(16),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_extra);

        let leaf_low = Node::new_leaf(
            Id(1),
            vec![5, 6, 0, 0],
            vec![
                Record::from_u32(5),
                Record::from_u32(6),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );
        storage.add_node(&leaf_low);

        let leaf_high = Node::new_leaf(
            Id(2),
            vec![9, 10, 0, 0],
            vec![
                Record::from_u32(9),
                Record::from_u32(10),
                Record::from_u32(0),
                Record::from_u32(0),
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
                    Record::from_u32(5),
                    Record::from_u32(9),
                    Record::from_u32(10),
                    Record::from_u32(0),
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
                assert_eq!(res.unwrap().into_u32(), i);
            }

            for i in 2..=key {
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_some());
                assert_eq!(find_res.unwrap().into_u32(), i);
                // /                println!("remove {:?}", i);

                let str_before = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("before"),
                );

                let remove_res = crate::remove::remove_key(&mut storage, &root_node, i);
                if remove_res.is_err() {
                    println!("error: {:?}", remove_res.err());
                    assert!(false);
                } else {
                    root_node = remove_res.unwrap();
                }
                let str_after = debug::storage_to_string(
                    &storage,
                    root_node.clone(),
                    true,
                    &String::from("after"),
                );

                let mut mapped_values = Vec::new();
                map(&mut storage, &root_node, i, key, &mut |k, v| {
                    assert_eq!(v.into_u32(), k);
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
                    if d.into_u32() != k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_u32(), k);
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
                assert_eq!(res.unwrap().into_u32(), i);
            }

            for i in (2..=key).rev() {
                let find_res = find(&mut storage, &root_node, i)?;
                assert!(find_res.is_some());
                assert_eq!(find_res.unwrap().into_u32(), i);
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
                    assert_eq!(v.into_u32(), k);
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
                    if d.into_u32() != k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_u32(), k);
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
                assert_eq!(res.unwrap().into_u32(), i);
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
                assert_eq!(find_res.unwrap().into_u32(), i);
                println!(">> {} {} remove {:?} size: {}", hight, t, i, storage.size());
                if hight == 6 && i == 11 {
                    println!("!");
                }
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
                            assert_eq!(v.into_u32(), k);
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
                    if d.into_u32() != *k {
                        debug::print_state(&str_before, &str_after);
                    }
                    assert_eq!(d.into_u32(), *k);
                }
            }
        }
        return Ok(());
    }

    fn remove_by_list(t: usize, nums: Vec<u32>) -> Result<()> {
        println!("nums: {:?}", nums.len());
        print!("t:{}", t);
        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let params = TreeParams::default_with_t(t).with_min_size_root(2);
        let mut storage: MockNodeStorage = MockNodeStorage::new(params);
        storage.add_node(&root_node);

        for i in &nums {
            // if *i == 8 {
            //     println!("")
            // }
            //let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));
            let res = insert(&mut storage, &root_node, *i, &Record::from_u32(*i));
            //crate::helpers::print_state(&str_before, &String::from(""));
            assert!(res.is_ok());
            root_node = res.unwrap();
        }

        let str_before =
            debug::storage_to_string(&storage, root_node.clone(), true, &String::from("before"));

        for i in &nums {
            let res = find(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("");
                println!("> not found {}", i);
            }
            assert!(res.is_ok());
            let v = res.unwrap();
            if !v.is_some() {
                println!("not found {}", *i);
                debug::print_state(&str_before, &String::from(""));
                assert!(false);
            }
            assert!(v.is_some());
            let rec = v.unwrap();
            assert_eq!(rec.into_u32(), *i);
        }

        let mut removed = HashSet::new();

        for i in &nums {
            println!("><> {}", *i);

            removed.insert(*i);
            let str_before = debug::storage_to_string(
                &storage,
                root_node.clone(),
                true,
                &String::from("before"),
            );
            let res = remove_key(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("> not found {}", i);
                assert!(false);
            }
            assert!(res.is_ok());
            root_node = res.unwrap();

            let str_after =
                debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));
            for item in &nums {
                if removed.contains(item) {
                    continue;
                }

                let res = find(&mut storage, &root_node, *item);
                if res.is_err() {
                    println!("> error {}", *item);
                }

                if res.unwrap().is_none() {
                    debug::print_state(&str_before, &str_after);
                    println!("> error {}", *item);
                    return Err(types::Error("".to_owned()));
                }
            }
        }
        Ok(())
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

        let res = insert(&mut storage, &root_node, 1, &Record::from_u32(1));
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
            assert_eq!(find_res.unwrap().unwrap().into_u32(), i);
        }
        return Ok(());
    }

    #[test]
    fn remove_by_numlist_1() -> Result<()> {
        let nums = vec![
            381, 147, 372, 83, 191, 338, 40, 141, 289, 76, 188, 257, 154, 38, 72, 112, 125, 306,
            255, 184, 81, 143, 132, 370, 177, 108, 324, 120, 155, 205, 36, 115, 355, 318, 219, 346,
            58, 365, 233, 52, 70, 167, 88, 197, 92, 95, 389, 304, 270, 312, 245, 314, 398, 291,
            369, 4, 256, 388, 263, 26, 301, 35, 302, 14, 56, 91, 303, 244, 400, 87, 278, 351, 227,
            29, 307, 163, 113, 249, 373, 391, 296, 190, 41, 333, 85, 272, 98, 126, 39, 243, 138,
            23, 22, 264, 228, 271, 215, 322, 75, 25, 171, 352, 371, 200, 376, 253, 18, 320, 327,
            336, 332, 349, 13, 218, 343, 64, 117, 356, 198, 382, 30, 347, 168, 374, 335, 79, 378,
            208, 178, 294, 47, 67, 173, 74, 90, 251, 89, 151, 337, 201, 86, 199, 237, 165, 282, 50,
            140, 9, 275, 298, 12, 390, 48, 37, 119, 134, 49, 238, 285, 242, 80, 19, 299, 300, 71,
            135, 385, 295, 54, 144, 20, 330, 66, 290, 326, 110, 202, 104, 317, 368, 273, 109, 180,
            287, 248, 43, 34, 105, 266, 357, 185, 362, 123, 8, 136, 6, 159, 31, 146, 207, 107, 111,
            224, 153, 223, 366, 241, 158, 162, 345, 203, 286, 252, 254, 232, 94, 209, 288, 137,
            100, 353, 68, 210, 397, 292, 258, 103, 5, 16, 99, 93, 230, 160, 44, 57, 55, 259, 367,
            78, 239, 283, 277, 150, 61, 323, 77, 7, 196, 360, 269, 10, 350, 386, 348, 206, 250,
            204, 325, 316, 354, 129, 262, 342, 101, 53, 65, 2, 28, 221, 97, 246, 176, 164, 226,
            339, 193, 170, 309, 174, 305, 383, 276, 396, 399, 260, 128, 130, 394, 152, 189, 82,
            133, 280, 265, 319, 69, 139, 297, 361, 240, 186, 102, 145, 187, 179, 212, 24, 384, 122,
            214, 377, 32, 395, 392, 393, 359, 1, 222, 195, 73, 334, 182, 315, 157, 311, 229, 62,
            183, 27, 216, 114, 121, 380, 156, 341, 46, 331, 225, 293, 175, 169, 358, 274, 267, 231,
            308, 166, 96, 11, 131, 149, 17, 192, 236, 328, 127, 364, 181, 148, 161, 142, 234, 344,
            247, 321, 84, 51, 379, 118, 15, 33, 363, 329, 220, 284, 21, 60, 124, 217, 106, 211,
            281, 116, 172, 45, 310, 340, 59, 268, 375, 63, 3, 235, 42, 313, 279, 387, 194, 261,
            213,
        ];
        remove_by_list(4, nums)?;
        Ok(())
    }
    #[test]
    fn remove_bu_numlist_2() -> Result<()> {
        let nums = vec![
            47, 41, 177, 559, 605, 331, 989, 183, 670, 476, 233, 142, 870, 417, 34, 553, 165, 598,
            854, 791, 155, 935, 721, 437, 600, 378, 992, 701, 42, 928, 965, 328, 837, 741, 958, 44,
            355, 613, 48, 625, 593, 814, 555, 815, 523, 396, 492, 895, 781, 134, 357, 465, 90, 941,
            609, 14, 733, 130, 348, 760, 892, 493, 343, 270, 185, 536, 223, 859, 136, 506, 635,
            879, 358, 151, 23, 817, 748, 461, 875, 431, 923, 289, 463, 420, 382, 835, 176, 60, 416,
            825, 861, 112, 281, 426, 969, 471, 69, 819, 86, 30, 421, 330, 167, 316, 831, 956, 910,
            7, 38, 726, 250, 439, 863, 447, 184, 308, 504, 370, 710, 629, 614, 957, 620, 221, 195,
            567, 796, 651, 578, 211, 722, 150, 486, 322, 558, 53, 204, 10, 939, 860, 765, 287, 891,
            359, 858, 307, 806, 725, 339, 84, 503, 57, 424, 530, 630, 878, 844, 695, 767, 309, 365,
            968, 518, 31, 702, 959, 995, 565, 993, 599, 366, 554, 73, 978, 974, 649, 566, 700, 883,
            707, 429, 912, 59, 110, 511, 83, 152, 342, 687, 455, 166, 608, 108, 694, 924, 642, 71,
            137, 971, 531, 920, 866, 325, 318, 576, 262, 337, 758, 526, 596, 644, 622, 100, 207,
            294, 569, 572, 903, 786, 761, 550, 897, 621, 227, 594, 169, 704, 423, 49, 590, 538,
            539, 430, 92, 922, 984, 686, 774, 459, 619, 584, 219, 808, 933, 890, 98, 386, 115, 943,
            253, 624, 663, 647, 174, 510, 118, 468, 497, 87, 507, 62, 783, 678, 537, 302, 955, 633,
            249, 659, 107, 121, 772, 400, 181, 543, 713, 145, 697, 646, 921, 966, 171, 512, 987,
            451, 375, 443, 156, 662, 398, 452, 792, 828, 397, 587, 173, 61, 661, 712, 347, 102,
            126, 945, 222, 900, 293, 488, 709, 395, 936, 832, 251, 33, 564, 123, 139, 513, 706,
            997, 884, 218, 604, 601, 821, 479, 440, 40, 1000, 372, 591, 612, 560, 50, 905, 288,
            300, 385, 409, 368, 180, 779, 351, 745, 79, 453, 186, 692, 770, 72, 588, 602, 93, 80,
            948, 20, 509, 464, 708, 332, 239, 998, 869, 327, 927, 744, 127, 393, 952, 483, 94, 484,
            303, 297, 470, 317, 847, 586, 677, 795, 161, 466, 267, 290, 25, 35, 868, 794, 842, 27,
            163, 676, 656, 141, 418, 556, 410, 326, 99, 356, 116, 284, 988, 474, 823, 552, 158,
            931, 639, 981, 719, 192, 650, 673, 179, 341, 925, 581, 6, 230, 56, 658, 208, 737, 361,
            113, 432, 849, 778, 67, 88, 852, 68, 442, 477, 907, 406, 668, 571, 534, 946, 380, 681,
            574, 950, 200, 29, 229, 763, 535, 411, 24, 1, 401, 865, 887, 119, 718, 371, 784, 750,
            932, 487, 51, 403, 473, 320, 122, 450, 982, 983, 508, 367, 101, 189, 264, 247, 495,
            626, 873, 444, 462, 140, 864, 833, 276, 874, 738, 551, 353, 841, 399, 8, 631, 153, 627,
            114, 65, 482, 428, 246, 638, 315, 545, 340, 214, 449, 43, 850, 283, 346, 802, 240, 168,
            617, 345, 607, 335, 234, 757, 855, 937, 754, 412, 404, 252, 637, 257, 636, 427, 209,
            220, 310, 603, 994, 82, 128, 3, 840, 542, 52, 19, 70, 54, 39, 458, 764, 197, 9, 496,
            685, 669, 76, 364, 942, 743, 582, 45, 17, 664, 592, 977, 172, 986, 5, 843, 26, 336,
            106, 913, 846, 244, 880, 611, 279, 258, 898, 274, 157, 243, 381, 919, 953, 724, 845,
            387, 610, 867, 363, 178, 816, 22, 266, 21, 824, 475, 980, 159, 714, 985, 469, 392, 18,
            2, 521, 790, 299, 188, 12, 228, 268, 751, 771, 147, 436, 653, 376, 570, 682, 908, 929,
            643, 261, 37, 671, 732, 446, 305, 132, 319, 517, 648, 210, 425, 198, 467, 972, 433, 66,
            77, 749, 657, 273, 976, 354, 277, 304, 383, 225, 260, 349, 263, 91, 193, 800, 369, 820,
            129, 235, 640, 789, 918, 804, 374, 224, 391, 674, 182, 245, 548, 389, 525, 964, 773,
            589, 438, 690, 703, 373, 460, 298, 916, 723, 413, 146, 776, 652, 996, 89, 138, 196,
            217, 448, 547, 606, 769, 740, 323, 541, 259, 265, 727, 478, 911, 74, 577, 716, 491,
            654, 909, 886, 914, 848, 585, 206, 311, 780, 755, 618, 762, 63, 149, 231, 798, 11, 871,
            856, 514, 728, 809, 979, 705, 615, 583, 527, 295, 973, 111, 498, 187, 827, 15, 472,
            269, 597, 715, 881, 384, 962, 445, 256, 967, 926, 213, 408, 877, 735, 822, 899, 813,
            216, 544, 456, 254, 645, 515, 991, 58, 934, 120, 944, 557, 528, 811, 696, 502, 862,
            135, 324, 272, 435, 292, 85, 915, 124, 524, 441, 616, 788, 742, 352, 362, 78, 970, 46,
            241, 248, 561, 793, 838, 736, 641, 893, 278, 205, 529, 947, 405, 321, 201, 237, 960,
            917, 522, 818, 999, 519, 949, 698, 291, 314, 683, 454, 666, 28, 226, 434, 655, 402,
            105, 801, 579, 286, 388, 481, 117, 568, 768, 55, 16, 344, 797, 338, 500, 684, 494, 144,
            13, 872, 787, 562, 901, 520, 660, 766, 573, 851, 95, 282, 109, 212, 296, 889, 379, 236,
            501, 930, 407, 505, 731, 394, 782, 753, 759, 693, 313, 457, 717, 672, 810, 489, 836,
            667, 990, 885, 170, 963, 419, 747, 190, 839, 97, 834, 175, 688, 36, 280, 729, 756, 215,
            876, 634, 904, 516, 96, 480, 699, 812, 689, 329, 199, 739, 103, 104, 785, 975, 81, 888,
            938, 830, 857, 680, 746, 4, 301, 940, 679, 711, 143, 490, 734, 775, 546, 853, 533, 131,
            954, 64, 485, 580, 563, 133, 125, 334, 160, 803, 777, 306, 285, 203, 238, 951, 275,
            902, 499, 232, 896, 415, 360, 390, 202, 540, 333, 829, 730, 549, 32, 242, 628, 162,
            312, 148, 807, 894, 595, 422, 720, 882, 623, 575, 194, 799, 752, 191, 826, 961, 350,
            377, 414, 665, 164, 154, 906, 271, 805, 691, 675, 532, 75, 255, 632,
        ];
        remove_by_list(4, nums)?;
        Ok(())
    }
}
