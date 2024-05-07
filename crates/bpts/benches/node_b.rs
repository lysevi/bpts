use criterion::{black_box, criterion_group, criterion_main, Criterion};

use bpts::tree::node;
use bpts::tree::record::Record;
use bpts::types::Id;

pub struct MockKeyCmp {}
impl MockKeyCmp {
    pub fn new() -> MockKeyCmp {
        MockKeyCmp {}
    }
}

impl node::NodeKeyCmp for MockKeyCmp {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        key1.cmp(&key2)
    }
}

fn find_in_node(target: &node::Node, cmp: &dyn node::NodeKeyCmp, keys: &Vec<u32>) {
    for key in keys.iter() {
        target.find(cmp, *key);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let sz = 10000;
    let mut keys = Vec::with_capacity(sz);
    let mut data = Vec::with_capacity(sz);
    for i in 0..sz {
        keys.push(i as u32);
        data.push(Record::from_u32(i as u32));
    }
    let leaf = node::Node::new_leaf(Id::empty(), keys.clone(), data, sz, sz);
    let r = leaf.borrow();
    c.bench_function(" 100find", |b| {
        b.iter(|| find_in_node(&r, &MockKeyCmp::new(), black_box(&keys)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
