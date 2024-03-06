use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    rec::Record,
    types::Id,
    utils, Result,
};

pub fn split_node(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode> {
    // println!(        "split:is_leaf:{} target:{:?}",        target_node.borrow().is_leaf,        target_node.borrow().id    );
    let parent_node: RcNode;
    let is_new_root;
    if target_node.borrow().parent.is_empty()
    /*|| ref_target.is_leaf */
    {
        // create new_root
        is_new_root = true;

        parent_node = Node::new_root(
            storage.get_new_id(),
            vec![0i32; target_node.borrow().keys.capacity()],
            Record::empty_array(target_node.borrow().data.len()),
            0,
            0,
        );
        storage.add_node(&parent_node);
        // println!("split new root: {:?}", parent_node.borrow().id);
    } else {
        // println!("split exists root {:?}", target_node.borrow().parent);

        is_new_root = false;
        parent_node = storage.get_node(target_node.borrow().parent)?;
    }

    let mut new_keys = vec![0i32; target_node.borrow().keys.capacity()];
    let mut new_data = Record::empty_array(target_node.borrow().data.len());

    let mut brother_keys_count = t;
    let brother_data_count = t;
    target_node.borrow_mut().keys_count = t;
    target_node.borrow_mut().data_count = t;
    let mut ignore_middle_key = 0;
    if !target_node.borrow().is_leaf {
        // println!("split target is middle");
        brother_keys_count -= 1;
        target_node.borrow_mut().keys_count -= 1;
        ignore_middle_key = 1;
    }

    let middle_key = target_node.borrow().keys[t - ignore_middle_key];
    {
        for i in 0..brother_keys_count {
            new_keys[i] = target_node.borrow().keys[i + t];
        }

        for i in 0..brother_data_count {
            new_data[i] = target_node.borrow().data[i + t].clone();
        }
    }

    let new_brother: RcNode;
    let new_id = storage.get_new_id();
    if target_node.borrow().is_leaf {
        new_brother = Node::new_leaf(
            new_id,
            new_keys,
            new_data,
            brother_keys_count,
            brother_data_count,
        )
    } else {
        let target_id = target_node.borrow().id;
        for i in 0..brother_data_count {
            let child_num = new_data[i].into_id();
            if child_num != target_id {
                let child = storage.get_node(child_num)?;
                child.borrow_mut().parent = new_id;
            }
        }

        new_brother = Node::new_root(
            new_id,
            new_keys,
            new_data,
            brother_keys_count,
            brother_data_count,
        )
    }
    // println!("split new brother id: {:?}", new_id);
    {
        storage.add_node(&new_brother);
        let mut ref_to_brother = new_brother.borrow_mut();
        ref_to_brother.parent = parent_node.borrow().id;
        target_node.borrow_mut().parent = parent_node.borrow().id;

        ref_to_brother.right = target_node.borrow().right;
        if ref_to_brother.right.exists() {
            //TODO! check result
            let right_brother = storage.get_node(ref_to_brother.right)?;
            right_brother.borrow_mut().left = new_id;
        }
        target_node.borrow_mut().right = ref_to_brother.id;
        ref_to_brother.left = target_node.borrow().id;
    }
    //TODO! check result

    //let lowest_key = ref_to_brother.keys[0];
    if is_new_root {
        let mut ref_to_parent = parent_node.borrow_mut();
        ref_to_parent.keys[0] = middle_key;
        ref_to_parent.keys_count = 1;

        ref_to_parent.data[0] = Record::from_id(target_node.borrow().id);
        ref_to_parent.data[1] = Record::from_id(new_brother.borrow().id);
        ref_to_parent.data_count = 2;

        return Ok(parent_node.clone());
    } else {
        //TODO! check result;
        let can_insert = parent_node.borrow().can_insert(t);
        {
            let mut ref_to_parent = parent_node.borrow_mut();

            insert_key_to_parent(&mut ref_to_parent, middle_key, new_brother.borrow().id);
            ref_to_parent.keys_count += 1;
            ref_to_parent.data_count += 1;
        }

        if can_insert {
            return Ok(toproot.unwrap().clone());
        } else {
            return split_node(storage, &parent_node, t, toproot);
        }
    }
}

fn insert_key_to_parent(target_node: &mut Node, key: i32, id: Id) {
    let mut pos = 0usize;
    for _i in 0..target_node.keys_count {
        if target_node.keys[pos] >= key {
            break;
        }
        pos += 1;
    }

    utils::insert_to_array(&mut target_node.keys, pos, key);
    utils::insert_to_array(&mut target_node.data, pos + 1, Record::from_id(id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::read::{self, find};
    use crate::types;
    fn check_link_to_brother(storage: &MockNodeStorage) {
        let all_links_exists = storage.all(|n| {
            let n = n.borrow();
            return n.parent.is_empty()
                || (n.left.exists() && n.parent.exists())
                || (n.right.exists() && n.parent.exists());
        });

        assert!(all_links_exists);
    }
    #[test]
    fn split_leaf() -> Result<()> {
        let leaf1 = Node::new_leaf(
            types::Id(1),
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);

        let root = split_node(&mut storage, &leaf1, 3, None)?;

        assert_eq!(root.borrow().is_leaf, false);
        assert_eq!(root.borrow().keys_count, 1);
        assert_eq!(root.borrow().data_count, 2);

        let subtree1_res = storage.get_node(root.borrow().data[0].into_id());
        assert!(subtree1_res.is_ok());
        {
            let node = subtree1_res.unwrap();
            let keys_count = node.borrow().keys_count;
            let data_count = node.borrow().data_count;
            assert_eq!(keys_count, 3);
            assert_eq!(node.borrow().keys[0..keys_count], vec![1, 2, 3]);
            assert_eq!(data_count, 3);
            assert_eq!(
                node.borrow().data[0..data_count],
                vec![
                    Record::from_i32(1),
                    Record::from_i32(2),
                    Record::from_i32(3),
                ]
            );
        }
        let subtree2_res = storage.get_node(root.borrow().data[1].into_id());
        assert!(subtree2_res.is_ok());
        {
            let node = subtree2_res.unwrap();
            let keys_count = node.borrow().keys_count;
            let data_count = node.borrow().data_count;
            assert_eq!(keys_count, 3);
            assert_eq!(node.borrow().keys[0..keys_count], vec![4, 5, 6]);
            assert_eq!(data_count, 3);
            assert_eq!(
                node.borrow().data[0..data_count],
                vec![
                    Record::from_i32(4),
                    Record::from_i32(5),
                    Record::from_i32(6),
                ]
            );
        }

        let res = read::find(&mut storage, &root, 1)?;
        assert!(res.is_some());
        assert_eq!(res.unwrap(), Record::from_i32(1));

        check_link_to_brother(&storage);

        for i in [1, 2, 3, 4, 5, 6] {
            let r = find(&mut storage, &root, i);
            assert!(r.is_ok());
        }
        return Ok(());
    }

    #[test]
    fn split_full_middle() {
        /*
             5,  8, 11, 14, 17, 0     =>
            1, 3,  4,  5,  6 ,7

                    11
             5, 8,      14, 17
            1, 3, 4    5,  6, 7
        */
        let first_root_node = Node::new_root(
            types::Id(777),
            vec![5, 8, 11, 14, 17, 0],
            vec![
                Record::from_id(Id(1)),
                Record::from_id(Id(3)),
                Record::from_id(Id(4)),
                Record::from_id(Id(5)),
                Record::from_id(Id(6)),
                Record::from_id(Id(7)),
            ],
            5,
            6,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&first_root_node);

        {
            let ref_to_node = first_root_node.borrow_mut();
            for i in &ref_to_node.data {
                let new_leaf =
                    Node::new_leaf(i.into_id(), vec![0], vec![Record::from_i32(1)], 1, 1);
                new_leaf.borrow_mut().left = types::Id(999);
                storage.add_node(&new_leaf);
            }
        }
        let result = split_node(&mut storage, &first_root_node, 3, None);
        let root = result.unwrap();

        assert_ne!(root.borrow().id, types::Id(1));
        assert_eq!(root.borrow().is_leaf, false);
        assert_eq!(root.borrow().keys_count, 1);
        assert_eq!(root.borrow().keys[0], 11);
        assert_eq!(root.borrow().data_count, 2);

        let id_1 = root.borrow().data[0].into_id();
        let id_2 = root.borrow().data[1].into_id();
        let subtree1_res = storage.get_node(id_1);
        let subtree2_res = storage.get_node(id_2);
        assert!(subtree1_res.is_ok());
        {
            let node = subtree1_res.unwrap();
            let keys_count = node.borrow().keys_count;
            let data_count = node.borrow().data_count;
            assert_eq!(keys_count, 2);
            assert_eq!(node.borrow().keys[0..keys_count], vec![5, 8]);
            assert_eq!(data_count, 3);
            assert_eq!(
                node.borrow().data[0..data_count],
                vec![
                    Record::from_id(Id(1)),
                    Record::from_id(Id(3)),
                    Record::from_id(Id(4))
                ]
            );
        }

        assert!(subtree2_res.is_ok());
        {
            let node = subtree2_res.unwrap();
            let keys_count = node.borrow().keys_count;
            let data_count = node.borrow().data_count;
            assert_eq!(keys_count, 2);
            assert_eq!(node.borrow().keys[0..keys_count], vec![14, 17]);
            assert_eq!(data_count, 3);
            assert_eq!(
                node.borrow().data[0..data_count],
                vec![
                    Record::from_id(Id(5)),
                    Record::from_id(Id(6)),
                    Record::from_id(Id(7)),
                ]
            );
        }

        check_link_to_brother(&storage);
    }

    #[test]
    fn add_key_to_parent() {
        let keys = vec![13, 24, 0, 0];
        let data = vec![
            Record::from_id(types::Id(2)),
            Record::from_id(types::Id(13)),
            Record::from_id(types::Id(24)),
            Record::from_id(types::Id(0)),
            Record::from_id(types::Id(0)),
        ];
        let leaf = Node::new_leaf(types::Id(1), keys, data, 2, 3);
        let mut ref_to_leaf = leaf.borrow_mut();
        insert_key_to_parent(&mut ref_to_leaf, 19, types::Id(19));

        assert_eq!(ref_to_leaf.keys, vec![13, 19, 24, 0]);
        assert_eq!(
            ref_to_leaf.data,
            vec![
                Record::from_id(types::Id(2)),
                Record::from_id(types::Id(13)),
                Record::from_id(types::Id(19)),
                Record::from_id(types::Id(24)),
                Record::from_id(types::Id(0)),
            ]
        );
    }
    #[test]
    fn split_leaf_with_exists_parent() {
        /*
                     11
        1,2,3,4,5,6     12,13
        1,2,3,4,5,6     12 13

              4        11
        1,2,3,  4,5,6     12,13
        1,2,3,  4,5,6     12 13
        */
        let root_node = Node::new_root(
            types::Id(1),
            vec![11],
            vec![Record::from_i32(1), Record::from_i32(2)],
            1,
            2,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let leaf1_node = Node::new_leaf(
            types::Id(2),
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
            7,
        );
        leaf1_node.borrow_mut().parent = types::Id(1);
        leaf1_node.borrow_mut().right = types::Id(3);
        storage.add_node(&leaf1_node);

        let leaf2_node = Node::new_root(
            types::Id(3),
            vec![11],
            vec![Record::from_i32(1), Record::from_i32(2)],
            1,
            2,
        );
        leaf1_node.borrow_mut().parent = types::Id(1);
        leaf1_node.borrow_mut().left = types::Id(2);
        storage.add_node(&leaf2_node);

        let result = split_node(&mut storage, &leaf1_node, 3, Some(root_node.clone()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().borrow().id, root_node.borrow().id);
        assert_eq!(storage.size(), 4);

        check_link_to_brother(&storage);
    }
}
