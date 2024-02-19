use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    rec::Record,
    types::{self, Id},
    utils,
};

pub fn split_node(
    storage: &mut dyn NodeStorage,
    target_node: &mut Node,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    let parent_node: RcNode;
    let is_new_root;
    if target_node.parent == types::EMPTY_ID
    /*|| ref_target.is_leaf */
    {
        // create new_root
        is_new_root = true;
        let new_data = Record::empty_array(target_node.data.len(), target_node.data[0].size());
        parent_node = Node::new_root(
            storage.get_new_id(),
            vec![0i32; target_node.keys.capacity()],
            new_data,
            0,
            0,
        );
        storage.add_node(&parent_node);
    } else {
        //TODO! check unwrap
        is_new_root = false;
        parent_node = storage.get_node(&target_node.parent).unwrap();
    }

    let mut new_keys = vec![0i32; target_node.keys.capacity()];
    let mut new_data = Record::empty_array(target_node.data.len(), target_node.data[0].size());

    let mut brother_keys_count = t;
    let brother_data_count = t;
    target_node.keys_count = t;
    target_node.data_count = t;
    let mut ignore_middle_key = 0;
    if !target_node.is_leaf {
        // ref_target.keys_count -= 1;
        //ref_target.data_count += 1;
        brother_keys_count -= 1;
        target_node.keys_count -= 1;
        ignore_middle_key = 1;
    }

    let middle_key = target_node.keys[t - ignore_middle_key];
    {
        for i in 0..brother_keys_count {
            new_keys[i] = target_node.keys[i + t];
        }

        for i in 0..brother_data_count {
            new_data[i] = target_node.data[i + t].clone();
        }
    }

    let new_brother: RcNode;
    let new_id = storage.get_new_id();
    if target_node.is_leaf {
        new_brother = Node::new_leaf(
            new_id,
            new_keys,
            new_data,
            brother_keys_count,
            brother_data_count,
        )
    } else {
        new_brother = Node::new_root(
            new_id,
            new_keys,
            new_data,
            brother_keys_count,
            brother_data_count,
        )
    }
    storage.add_node(&new_brother);
    let mut ref_to_brother = new_brother.borrow_mut();
    ref_to_brother.parent = parent_node.borrow().id;
    target_node.parent = parent_node.borrow().id;
    //TODO! check result

    //let lowest_key = ref_to_brother.keys[0];
    if is_new_root {
        let mut ref_to_parent = parent_node.borrow_mut();
        ref_to_parent.keys[0] = middle_key;
        ref_to_parent.keys_count = 1;

        ref_to_parent.data[0] = Record::from_id(target_node.id);
        ref_to_parent.data[1] = Record::from_id(ref_to_brother.id);
        ref_to_parent.data_count = 2;

        return Ok(parent_node.clone());
    } else {
        //TODO! check result;
        let can_insert = parent_node.borrow().can_insert(t);
        {
            let mut ref_to_parent = parent_node.borrow_mut();

            insert_key_to_parent(&mut ref_to_parent, middle_key, ref_to_brother.id);
            ref_to_parent.keys_count += 1;
            ref_to_parent.data_count += 1;
        }

        if can_insert {
            return Ok(toproot.unwrap().clone());
        } else {
            return split_node(storage, &mut parent_node.borrow_mut(), t, toproot);
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
    use crate::read;
    #[test]
    fn split_leaft() {
        let leaf1 = Node::new_leaf(
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
        storage.add_node(&leaf1);

        let result = split_node(&mut storage, &mut leaf1.borrow_mut(), 3, None);
        if let Ok(root) = result {
            assert_eq!(root.borrow().is_leaf, false);
            assert_eq!(root.borrow().keys_count, 1);
            assert_eq!(root.borrow().data_count, 2);

            let subtree1_res = storage.get_node(&root.borrow().data[0].into_id());
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
                    vec![Record::from_u8(1), Record::from_u8(2), Record::from_u8(3),]
                );
            }
            let subtree2_res = storage.get_node(&root.borrow().data[1].into_id());
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
                    vec![Record::from_u8(4), Record::from_u8(5), Record::from_u8(6),]
                );
            }

            let res = read::find(&mut storage, &root, 1);
            assert!(res.is_ok());
            assert_eq!(res.unwrap(), Record::from_u8(1));
        } else {
            assert!(false);
        }
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
        let root_node = Node::new_root(
            types::Id(1),
            vec![5, 8, 11, 14, 17, 0],
            vec![
                Record::from_u8(1),
                Record::from_u8(3),
                Record::from_u8(4),
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
            ],
            5,
            6,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let result = split_node(&mut storage, &mut root_node.borrow_mut(), 3, None);
        if let Ok(root) = result {
            assert_eq!(root.borrow().is_leaf, false);
            assert_eq!(root.borrow().keys_count, 1);
            assert_eq!(root.borrow().keys[0], 11);
            assert_eq!(root.borrow().data_count, 2);

            let subtree1_res = storage.get_node(&root.borrow().data[0].into_id());
            let subtree2_res = storage.get_node(&root.borrow().data[1].into_id());
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
                    vec![Record::from_u8(1), Record::from_u8(3), Record::from_u8(4)]
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
                    vec![Record::from_u8(5), Record::from_u8(6), Record::from_u8(7),]
                );
            }
        } else {
            assert!(false);
        }
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
            vec![Record::from_u8(1), Record::from_u8(2)],
            1,
            2,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let leaf1_node = Node::new_leaf(
            types::Id(2),
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
            7,
        );
        leaf1_node.borrow_mut().parent = types::Id(1);
        leaf1_node.borrow_mut().right = types::Id(3);
        storage.add_node(&leaf1_node);

        let leaf2_node = Node::new_root(
            types::Id(3),
            vec![11],
            vec![Record::from_u8(1), Record::from_u8(2)],
            1,
            2,
        );
        leaf1_node.borrow_mut().parent = types::Id(1);
        leaf1_node.borrow_mut().left = types::Id(2);
        storage.add_node(&leaf2_node);

        let result = split_node(
            &mut storage,
            &mut leaf1_node.borrow_mut(),
            3,
            Some(root_node.clone()),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().borrow().id, root_node.borrow().id);
        assert_eq!(storage.size(), 4);
    }
}
