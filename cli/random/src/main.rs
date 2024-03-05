use std::io::Write;

use bpts::*;
use rand::prelude::*;
use std::time::Instant;

fn main() {
    let count = 50000;
    let mut rng = rand::thread_rng();

    let mut nums: Vec<i32> = (0..count).collect();
    nums.shuffle(&mut rng);
    println!("nums: {:?}", nums.len());
    for t in 4..500 {
        print!("t:{}", t);
        std::io::stdout().flush().unwrap();
        let start = Instant::now();
        let mut root_node = Node::new_leaf_with_size(Id(1), t);
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&root_node);

        for i in &nums {
            let res = insert(&mut storage, &root_node, *i, &Record::from_i32(*i), t);
            assert!(res.is_ok());
            root_node = res.unwrap();

            let res = bpts::find(&mut storage, &root_node, *i);
            if res.is_err() {
                println!("> not found {}", i);
            }
            assert!(res.is_ok());
        }
        let duration = start.elapsed();
        println!("\tstorage size:{} \telapsed:{:?}", storage.size(), duration);
    }
}
