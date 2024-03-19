use crate::{
    node::RcNode, nodestorage::NodeStorage, read, record::Record, split::split_node, Result,
};

pub fn insert<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: u32,
    value: &Record,
) -> Result<RcNode> {
    let target_node: RcNode;
    {
        if root.borrow().is_empty() {
            target_node = root.clone();
        } else {
            let scan_result = read::scan(storage, &root, key);
            if scan_result.is_err() {
                return scan_result;
            }

            target_node = scan_result.unwrap();
        }
        let cmp = storage.get_cmp();
        // println!("insert into {:?}", target_node.borrow().id);
        let mut mut_ref = target_node.borrow_mut();
        let can_insert = mut_ref.can_insert(storage.get_params().get_t());

        let mut index = mut_ref.keys_count;
        for i in 0..mut_ref.keys_count {
            if cmp.compare(mut_ref.keys[i], key).is_gt() {
                index = i;
                break;
            }

            if cmp.compare(mut_ref.keys[i], key).is_eq() {
                index = i;
                break;
            }
        }
        mut_ref.insert_data(index, key, value.clone());

        if can_insert {
            return Ok(root.clone());
        }
    }
    return split_node(storage, &target_node, Some(root.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    fn many_inserts(t: usize, maxnodecount: usize) -> Result<()> {
        let mut root_node = Node::new_leaf_with_size(Id(1), t);

        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(t));
        storage.add_node(&root_node);

        let mut key: u32 = 1;
        while storage.size() < maxnodecount {
            key += 1;
            println!("+ {:?} root:{:?}", key, root_node.borrow().id);
            if key == 22 {
                println!("kv 22");
            }
            let res = insert(&mut storage, &root_node, key, &Record::from_u32(key));
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in 2..=key {
                //println!("! {:?}", i);
                if key == 22 && i == 20 {
                    println!("!");
                }
                let res = find(&mut storage, &root_node, i)?;
                assert!(res.is_some());
                assert_eq!(res.unwrap().into_u32(), i);
            }
        }

        for i in 2..key {
            let res = find(&mut storage, &root_node, i)?;
            assert!(res.is_some());
            assert_eq!(res.unwrap().into_u32(), i);
        }

        let res = find(&mut storage, &root_node, key - 1)?;
        assert!(res.is_some());
        //println!(">> {:?}", res);
        let mut mapped_values = Vec::new();
        map(&mut storage, &root_node, 2, key - 1, &mut |k, v| {
            println!("mapped {:?}", k);
            assert_eq!(v.into_u32(), k);
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
            assert_eq!(v.into_u32(), k);
            mapped_values.push(k);
        })
        .unwrap();
        assert_eq!(mapped_values.len(), (key - 2) as usize);

        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] > mapped_values[i]);
        }
        Ok(())
    }

    fn many_inserts_back(t: usize, maxnodecount: usize) -> Result<()> {
        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(t));
        storage.add_node(&root_node);

        let mut keys = Vec::new();

        let mut key: u32 = std::u32::MAX - 1;
        let mut total_count = 0;
        while storage.size() < maxnodecount {
            total_count += 1;
            key -= 1;
            println!("insert {}", key);
            keys.push(key);
            let res = insert(&mut storage, &root_node, key, &Record::from_u32(key));
            assert!(res.is_ok());
            root_node = res.unwrap();

            for i in keys.iter() {
                // println!(">> {}", i);
                let res = find(&mut storage, &root_node, *i)?;
                assert!(res.is_some());
                assert_eq!(res.unwrap().into_u32(), *i);
            }
        }

        for i in keys.iter() {
            let res = find(&mut storage, &root_node, *i)?;
            assert!(res.is_some());
            assert_eq!(res.unwrap().into_u32(), *i);
        }

        let res = find(&mut storage, &root_node, key - 1);
        println!(">> {:?}", res);
        let mut mapped_values = Vec::new();
        map(
            &mut storage,
            &root_node,
            *keys.last().unwrap(),
            keys[0],
            &mut |k, v| {
                println!("mapped {:?}", k);
                assert_eq!(v.into_u32(), k);
                mapped_values.push(k);
            },
        )
        .unwrap();

        assert_eq!(mapped_values.len(), total_count);
        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] < mapped_values[i]);
        }

        mapped_values.clear();
        map_rev(
            &mut storage,
            &root_node,
            *keys.last().unwrap(),
            keys[0],
            &mut |k, v| {
                println!("mapped_rev {:?}", k);
                assert_eq!(v.into_u32(), k);
                mapped_values.push(k);
            },
        )
        .unwrap();
        assert_eq!(mapped_values.len(), total_count);

        for i in 1..mapped_values.len() {
            assert!(mapped_values[i - 1] > mapped_values[i]);
        }
        Ok(())
    }

    fn inserts_to_middle(key_from: u32, key_to: u32, t: usize) -> Result<()> {
        let mut ranges = Vec::new();
        ranges.push((key_from, key_to));

        let mut keys = Vec::new();
        while !ranges.is_empty() {
            let r = *ranges.first().unwrap();
            ranges.remove(0);

            let middle = (r.0 + (r.1 - r.0) / 2) as u32 + 1;

            let i1 = (r.0, middle);
            let i2 = (middle, r.1);
            println!("{:?} {:?}", i1, i2);
            if !keys.contains(&r.0) {
                keys.push(r.0);
            }
            if !keys.contains(&r.1) {
                keys.push(r.1);
            }
            if !keys.contains(&middle) {
                keys.push(middle);
            }
            if i1.1 - i1.0 > 2 {
                ranges.push(i1);
            }
            if i2.1 - i2.0 > 2 {
                ranges.push(i2);
            }
        }

        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(t));
        storage.add_node(&root_node);

        for i in 0..keys.len() {
            //println!("insert {}", keys[i]);
            let str_before = crate::prelude::debug::storage_to_string(
                &storage,
                root_node.clone(),
                true,
                &String::from("before"),
            );
            let res = insert(
                &mut storage,
                &root_node,
                keys[i],
                &Record::from_u32(keys[i]),
            );
            assert!(res.is_ok());
            root_node = res.unwrap();

            let str_after =
                debug::storage_to_string(&storage, root_node.clone(), true, &String::from("after"));

            for j in 0..i {
                let res = find(&mut storage, &root_node, keys[j]);
                if res.is_err() {
                    println!("> not found {}", keys[j]);
                    debug::print_state(&str_before, &str_after)
                }
                assert!(res.is_ok());
                assert_eq!(res.unwrap().unwrap().into_u32(), keys[j]);
            }
        }
        return Ok(());
    }

    #[test]
    fn insert_to_tree() -> Result<()> {
        let leaf1 = Node::new_leaf(
            Id(1),
            vec![2, 3, 0, 0, 0, 0],
            vec![
                Record::from_u32(2),
                Record::from_u32(3),
                Record::from_u32(0),
                Record::from_u32(0),
                Record::from_u32(0),
                Record::from_u32(0),
            ],
            2,
            2,
        );

        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        storage.add_node(&leaf1);

        let new_value = Record::from_u32(1);
        let mut result = insert(&mut storage, &leaf1, 1, &new_value);
        assert!(result.is_ok());
        let mut new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 3);

        result = insert(&mut storage, &leaf1, 5, &new_value);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 4);

        result = insert(&mut storage, &leaf1, 4, &new_value);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 5);

        {
            let r = new_root.borrow();
            for i in 0..r.keys_count {
                assert_eq!(r.keys[i], (i + 1) as u32)
            }
        }

        let new_data = Record::from_u32(6);
        result = insert(&mut storage, &leaf1, 6, &new_data);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert!(!new_root.borrow().is_leaf);
        let search_result = read::find(&mut storage, &new_root, 6)?;
        assert!(search_result.is_some());

        let unpacked = search_result.expect("!");
        assert_eq!(unpacked.into_u32(), 6);
        Ok(())
    }

    #[test]
    fn many_inserts_3_10() -> Result<()> {
        many_inserts(3, 10)
    }

    #[test]
    fn many_inserts_7_22() -> Result<()> {
        many_inserts(7, 22)
    }

    #[test]
    fn many_inserts_back_3_10() -> Result<()> {
        many_inserts_back(3, 10)
    }

    #[test]
    fn many_inserts_back_7_22() -> Result<()> {
        many_inserts_back(7, 22)
    }

    #[test]
    fn inserts_to_middle_1_100_3() -> Result<()> {
        inserts_to_middle(1, 100, 3)
    }

    #[test]
    fn inserts_to_middle_1_500_6() -> Result<()> {
        inserts_to_middle(1, 500, 6)
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
