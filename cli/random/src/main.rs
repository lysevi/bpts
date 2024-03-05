use std::io::Write;

use bpts::*;
use rand::prelude::*;
use std::time::Instant;

fn main() {
    let count = 1000;
    let mut rng = rand::thread_rng();

    let mut nums: Vec<i32> = (0..count).collect();
    nums.shuffle(&mut rng);
    println!("nums: {:?}", nums.len());
    // let t = 5;
    // nums = vec![9, 1, 7, 3, 4, 5, 2, 0, 8, 6];
    for t in 4..500 {
        print!("t:{}", t);
        std::io::stdout().flush().unwrap();
        let start = Instant::now();
        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        for i in &nums {
            // if *i == 8 {
            //     println!("")
            // }
            //let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));
            let res = insert(&mut storage, &root_node, *i, &Record::from_i32(*i), t);
            //MockNodeStorage::print_state(&str_before, &String::from(""));
            assert!(res.is_ok());
            root_node = res.unwrap();
        }

        let duration = start.elapsed();
        println!("\tstorage size:{} \telapsed:{:?}", storage.size(), duration);
        let str_before = storage.to_string(root_node.clone(), true, &String::from("before"));
        for i in &nums {
            let res = bpts::find(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("> not found {}", i);
            }
            assert!(res.is_ok());
            let v = res.unwrap();
            if !v.is_some() {
                println!("not found {}", *i);
                MockNodeStorage::print_state(&str_before, &String::from(""));
                return;
            }
            assert!(v.is_some());
            let rec = v.unwrap();
            assert_eq!(rec.into_i32(), *i);
        }
    }
}
