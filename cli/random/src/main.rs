use bpts::*;
use rand::prelude::*;

fn main() {
    let t = 10;
    let count = 100000;
    let mut root_node = Node::new_leaf_with_size(Id(1), t);
    let mut storage: MockNodeStorage = MockNodeStorage::new();
    storage.add_node(&root_node);

    let mut rng = rand::thread_rng();

    let mut nums: Vec<i32> = (1..count).collect();
    nums.shuffle(&mut rng);

    println!("nums: {:?}", nums);
    for i in nums {
        let res = insert(&mut storage, &root_node, i, &Record::from_i32(i), t);
        assert!(res.is_ok());
        root_node = res.unwrap();

        let res = bpts::find(&mut storage, &root_node, i);
        if res.is_err() {
            println!("> not found {}", i);
        }
        assert!(res.is_ok());
    }
}
