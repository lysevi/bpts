use std::{collections::HashSet, io::Write};

use bpts::tree::debug::{print_states, storage_to_string};
use bpts::tree::insert;
use bpts::tree::nodestorage::NodeStorage;
use bpts::tree::read::find;
use bpts::tree::record::Record;
use bpts::tree::remove::remove_key;
use bpts::{
    tree::{mocks::MockNodeStorage, node::Node, params::TreeParams},
    types::Id,
};
use rand::prelude::*;
use std::time::Instant;

fn main() {
    let count = 10000;
    let mut rng = rand::thread_rng();

    let mut nums: Vec<u32> = (1..=count).collect();
    nums.shuffle(&mut rng);

    println!("nums: {:?}", nums.len());
    // let t = 5;
    // nums = vec![9, 1, 7, 3, 4, 5, 2, 0, 8, 6];
    for t in 4..100 {
        print!("t:{}", t);
        std::io::stdout().flush().unwrap();
        let mut start = Instant::now();
        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let params = TreeParams::default_with_t(t).with_min_size_root(2);
        let mut storage: MockNodeStorage = MockNodeStorage::new(params);
        storage.add_node(&root_node);

        for i in &nums {
            // if *i == 8 {
            //     println!("")
            // }
            //let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));
            let res = insert::insert(&mut storage, &root_node, *i, &Record::from_u32(*i));
            //crate::helpers::print_state(&str_before, &String::from(""));
            assert!(res.is_ok());
            root_node = res.unwrap();
        }

        let mut duration = start.elapsed();
        print!("\tstorage size:{} \twrite:{:?}", storage.size(), duration);
        std::io::stdout().flush().unwrap();
        let str_before =
            storage_to_string(&storage, root_node.clone(), true, &String::from("before"));
        start = Instant::now();
        for i in &nums {
            let res = find(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("");
                println!("> not found {}", i);
                panic!();
            }
            assert!(res.is_ok());
            let v = res.unwrap();
            if !v.is_some() {
                println!("not found {}", *i);
                print_states(&[&str_before]);
                panic!();
            }
            assert!(v.is_some());
            let rec = v.unwrap();
            assert_eq!(rec.into_u32(), *i);
        }
        duration = start.elapsed();
        println!("\tread:{:?}", duration);
        std::io::stdout().flush().unwrap();

        start = Instant::now();
        let mut removed = HashSet::new();

        for i in &nums {
            // println!("><> {}", *i);
            // if *i == 373 {
            //     println!("!");
            // }

            removed.insert(*i);
            // let str_before = crate::debug::storage_to_string(
            //     &storage,
            //     root_node.clone(),
            //     true,
            //     &String::from("before"),
            // );
            let res = remove_key(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("> not found {}", i);
            }
            assert!(res.is_ok());
            root_node = res.unwrap();

            // let str_after = crate::debug::storage_to_string(
            //     &storage,
            //     root_node.clone(),
            //     true,
            //     &String::from("after"),
            // );
            for item in &nums {
                if removed.contains(item) {
                    continue;
                }

                let res = find(&mut storage, &root_node, *item);
                if res.is_err() {
                    println!("> error {}", *item);
                }

                if res.unwrap().is_none() {
                    //crate::debug::print_state(&str_before, &str_after);
                    println!("> not found {}", *item);
                    return;
                }
            }
            // let res = find(&mut storage, &root_node, *i);
            // if res.is_err() {
            //     println!("> error {}", i);
            // }
            //assert!(res.unwrap().is_none());
        }
        duration = start.elapsed();
        println!("\tremove:{:?}", duration);
        std::io::stdout().flush().unwrap();
    }
}
