use crate::{node::RcNode, nodestorage::NodeStorage, read, rec::Record, split::split_node, types};

pub fn insert(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
    value: &Record,
    t: usize,
) -> Result<RcNode, types::Error> {
    let target_node: RcNode;
    {
        if root.borrow().is_empty() {
            target_node = root.clone();
        } else {
            let scan_result = read::scan(storage, &root, key);
            if scan_result.is_err() {
                return Err(scan_result.err().unwrap());
            }

            target_node = scan_result.unwrap();
        }
        // println!("insert into {:?}", target_node.borrow().id);
        let mut mut_ref = target_node.borrow_mut();
        let can_insert = mut_ref.can_insert(t);

        let mut index = mut_ref.keys_count;
        for i in 0..mut_ref.keys_count {
            if mut_ref.keys[i] > key {
                index = i;
                break;
            }

            if mut_ref.keys[i] == 0 {
                index = i;
                break;
            }
        }
        mut_ref.insert_data(index, key, value.clone());

        if can_insert {
            return Ok(root.clone());
        }
    }
    return split_node(storage, &target_node, t, Some(root.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mocks::MockNodeStorage,
        node::Node,
        read::{self, find, map, map_rev},
        rec::Record,
    };

    fn many_inserts(t: usize, maxnodecount: usize) {
        let mut root_node = Node::new_leaf_with_size(types::Id(1), t);

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let mut key: i32 = 1;
        while storage.size() < maxnodecount {
            key += 1;
            println!("+ {:?} root:{:?}", key, root_node.borrow().id);
            if key == 22 {
                println!("kv 22");
            }
            let res = insert(&mut storage, &root_node, key, &Record::from_i32(key), t);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                //println!("! {:?}", i);
                if key == 22 && i == 20 {
                    println!("!");
                }
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }
        }

        for i in 2..key {
            let res = find(&mut storage, &root_node, i);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().into_i32(), i);
        }

        let res = find(&mut storage, &root_node, key - 1);
        println!(">> {:?}", res);
        let mut mapped_values = Vec::new();
        map(&mut storage, &root_node, 2, key - 1, &mut |k, v| {
            println!("mapped {:?}", k);
            assert_eq!(v.into_i32(), k);
            mapped_values.push(k);
        })
        .unwrap();
        assert_eq!(mapped_values.len(), (key - 2) as usize);

        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] < mapped_values[i]);
        }

        mapped_values.clear();
        map_rev(&mut storage, &root_node, 2, key - 1, &mut |k, v| {
            println!("mapped_rev {:?}", k);
            assert_eq!(v.into_i32(), k);
            mapped_values.push(k);
        })
        .unwrap();
        assert_eq!(mapped_values.len(), (key - 2) as usize);

        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] > mapped_values[i]);
        }
    }

    fn many_inserts_back(t: usize, maxnodecount: usize) {
        let mut root_node = Node::new_leaf_with_size(types::Id(1), t);
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        let mut key: i32 = 100;
        let mut total_count = 0;
        while storage.size() < maxnodecount {
            total_count += 1;
            key -= 1;
            println!("insert {}", key);
            let res = insert(&mut storage, &root_node, key, &Record::from_i32(key), t);
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in (key..99).rev() {
                // println!(">> {}", i);
                let res = find(&mut storage, &root_node, i);
                assert!(res.is_ok());
                assert_eq!(res.unwrap().into_i32(), i);
            }
        }

        for i in (key..99).rev() {
            let res = find(&mut storage, &root_node, i);
            assert!(res.is_ok());
            assert_eq!(res.unwrap().into_i32(), i);
        }

        let res = find(&mut storage, &root_node, key - 1);
        println!(">> {:?}", res);
        let mut mapped_values = Vec::new();
        map(&mut storage, &root_node, key, 99, &mut |k, v| {
            println!("mapped {:?}", k);
            assert_eq!(v.into_i32(), k);
            mapped_values.push(k);
        })
        .unwrap();

        assert_eq!(mapped_values.len(), total_count);
        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] < mapped_values[i]);
        }

        mapped_values.clear();
        map_rev(&mut storage, &root_node, key, 99, &mut |k, v| {
            println!("mapped_rev {:?}", k);
            assert_eq!(v.into_i32(), k);
            mapped_values.push(k);
        })
        .unwrap();
        assert_eq!(mapped_values.len(), total_count);

        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] > mapped_values[i]);
        }
    }

    #[test]
    fn insert_to_tree() {
        let leaf1 = Node::new_leaf(
            types::Id(1),
            vec![2, 3, 0, 0, 0, 0],
            vec![
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(0),
                Record::from_u8(0),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);

        let new_value = Record::from_u8(1);
        let mut result = insert(&mut storage, &leaf1, 1, &new_value, 3);
        assert!(result.is_ok());
        let mut new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 3);

        result = insert(&mut storage, &leaf1, 5, &new_value, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 4);

        result = insert(&mut storage, &leaf1, 4, &new_value, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 5);

        {
            let r = new_root.borrow();
            for i in 0..r.keys_count {
                assert_eq!(r.keys[i], (i + 1) as i32)
            }
        }

        let new_data = Record::from_u8(6);
        result = insert(&mut storage, &leaf1, 6, &new_data, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert!(!new_root.borrow().is_leaf);
        let search_result = read::find(&mut storage, &new_root, 6);
        assert!(search_result.is_ok());

        let unpacked = search_result.expect("!");
        assert_eq!(unpacked.into_u8(), 6u8);
    }

    #[test]
    fn many_inserts_3_10() {
        many_inserts(3, 10);
    }

    #[test]
    fn many_inserts_7_22() {
        many_inserts(7, 22);
    }

    #[test]
    fn many_inserts_back_3_10() {
        many_inserts_back(3, 10);
    }

    #[test]
    fn many_inserts_back_7_22() {
        many_inserts_back(7, 22);
    }

    #[test]
    #[ignore]
    fn insert_duplicate() {
        todo!();
    }

    #[test]
    #[ignore]
    fn bulk_write() {
        todo!()
    }
}
