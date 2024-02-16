use crate::{
    node::{Node, RcNode},
    nodestorage::NodeStorage,
    rec::Record,
    types, utils,
};

pub fn split_node(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    let parent_node: RcNode;
    let mut ref_target = target_node.borrow_mut();
    let is_new_root;
    if ref_target.parent == 0 || ref_target.is_leaf {
        // create new_root
        is_new_root = true;
        let new_data = Record::empty_array(ref_target.data.len(), ref_target.data[0].size());
        parent_node = Node::new_root(
            storage.get_new_id(),
            vec![0i32; ref_target.keys.capacity()],
            new_data,
            0,
            0,
        );
        storage.add_node(&parent_node);
    } else {
        //TODO! check unwrap
        is_new_root = false;
        parent_node = storage.get_node(ref_target.parent).unwrap();
    }

    let mut new_keys = vec![0i32; ref_target.keys.capacity()];
    let mut new_data = Record::empty_array(ref_target.data.len(), ref_target.data[0].size());

    let mut brother_keys_count = t;
    let brother_data_count = t;
    ref_target.keys_count = t;
    ref_target.data_count = t;
    let mut ignore_middle_key = 0;
    if !ref_target.is_leaf {
        // ref_target.keys_count -= 1;
        ref_target.data_count += 1;
        brother_keys_count -= 1;
        ignore_middle_key += 1;
    }

    let middle_key = ref_target.keys[t];
    {
        for i in 0..brother_keys_count {
            new_keys[i] = ref_target.keys[i + t + ignore_middle_key];
        }

        for i in 0..brother_data_count {
            new_data[i] = ref_target.data[i + t + ignore_middle_key].clone();
        }
    }

    let new_brother: RcNode;
    let new_id = storage.get_new_id();
    if ref_target.is_leaf {
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
    ref_target.parent = parent_node.borrow().id;
    //TODO! check result

    //let lowest_key = ref_to_brother.keys[0];
    if is_new_root {
        let mut ref_to_parent = parent_node.borrow_mut();
        ref_to_parent.keys[0] = middle_key;
        ref_to_parent.keys_count = 1;

        ref_to_parent.data[0] = Record::from_id(ref_target.id);
        ref_to_parent.data[1] = Record::from_id(ref_to_brother.id);
        ref_to_parent.data_count = 2;

        return Ok(parent_node.clone());
    } else {
        //TODO! check result;
        let parent = storage.get_node(ref_target.parent).unwrap();
        {
            ref_to_brother.parent = ref_target.parent;
            let mut ref_to_parent = parent.borrow_mut();
            let mut pos = 0;

            while pos < ref_to_parent.keys_count && ref_to_parent.keys[pos] < middle_key {
                pos += 1;
            }

            utils::insert_to_array(&mut ref_to_parent.keys, pos, middle_key);
            utils::insert_to_array(
                &mut ref_to_parent.data,
                pos + 1,
                Record::from_id(ref_to_brother.id),
            );
            ref_to_parent.keys_count += 1;
        }
        if parent.borrow().can_insert(t) {
            return Ok(toproot.unwrap().clone());
        } else {
            return split_node(storage, &parent, t, toproot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::read;
    #[test]
    fn split_leaft() {
        let leaf1 = Node::new_leaf(
            1,
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

        let result = split_node(&mut storage, &leaf1, 3, None);
        if let Ok(root) = result {
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
                    vec![Record::from_u8(1), Record::from_u8(2), Record::from_u8(3),]
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
    fn split_middle() {
        /*
            1, 2, 3, 4, 5, 6     =>
          0, 1, 2, 3, 4, 5, 6

                   4
            1, 2, 3      5, 6
           0, 1, 2, 3   4, 5, 6
        */
        let root_node = Node::new_root(
            1,
            vec![1, 2, 3, 4, 5, 6],
            vec![
                Record::from_u8(0),
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
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let result = split_node(&mut storage, &root_node, 3, None);
        if let Ok(root) = result {
            assert_eq!(root.borrow().is_leaf, false);
            assert_eq!(root.borrow().keys_count, 1);
            assert_eq!(root.borrow().keys[0], 4);
            assert_eq!(root.borrow().data_count, 2);

            let subtree1_res = storage.get_node(root.borrow().data[0].into_id());
            let subtree2_res = storage.get_node(root.borrow().data[1].into_id());
            assert!(subtree1_res.is_ok());
            {
                let node = subtree1_res.unwrap();
                let keys_count = node.borrow().keys_count;
                let data_count = node.borrow().data_count;
                assert_eq!(keys_count, 3);
                assert_eq!(node.borrow().keys[0..keys_count], vec![1, 2, 3]);
                assert_eq!(data_count, 4);
                assert_eq!(
                    node.borrow().data[0..data_count],
                    vec![
                        Record::from_u8(0),
                        Record::from_u8(1),
                        Record::from_u8(2),
                        Record::from_u8(3),
                    ]
                );
            }

            assert!(subtree2_res.is_ok());
            {
                let node = subtree2_res.unwrap();
                let keys_count = node.borrow().keys_count;
                let data_count = node.borrow().data_count;
                assert_eq!(keys_count, 2);
                assert_eq!(node.borrow().keys[0..keys_count], vec![5, 6]);
                assert_eq!(data_count, 3);
                assert_eq!(
                    node.borrow().data[0..data_count],
                    vec![Record::from_u8(4), Record::from_u8(5), Record::from_u8(6),]
                );
            }
        } else {
            assert!(false);
        }
    }
    #[test]
    #[ignore]
    fn split_middle_with_exists_parent() {
        todo!()
    }
}
