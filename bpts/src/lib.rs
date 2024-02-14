pub mod mfile;
pub mod rec;
pub mod types;
pub mod utils;

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use rec::Record;
use types::Id;

#[derive(Clone)]
pub struct Node {
    pub id: Id, //TODO remove
    pub is_leaf: bool,
    pub keys: Vec<i32>,
    pub data: Vec<Record>,
}

impl Node {
    pub fn new(id: Id, is_leaf: bool, keys: Vec<i32>, data: Vec<Record>) -> Rc<RefCell<Node>> {
        Rc::new(RefCell::new(Node {
            id: id,
            is_leaf: is_leaf,
            keys: keys,
            data: data,
        }))
    }

    pub fn new_root(id: Id, keys: Vec<i32>, data: Vec<Record>) -> Rc<RefCell<Node>> {
        Node::new(id, false, keys, data)
    }

    pub fn new_leaf(id: Id, keys: Vec<i32>, data: Vec<Record>) -> Rc<RefCell<Node>> {
        Node::new(id, true, keys, data)
    }

    pub fn find(&self, key: i32) -> Option<&Record> {
        if key < self.keys[0] {
            return self.data.first();
        }

        if !self.is_leaf {
            if let Some(last_key) = self.keys.last() {
                if *last_key <= key {
                    return self.data.last();
                }
            }
        }

        for i in 0..self.keys.len() {
            match (self.keys[i]).cmp(&key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Some(&self.data[i]),
                std::cmp::Ordering::Greater => return Some(&self.data[i]),
            }
        }
        return None;
    }
}

pub trait NodeStorage {
    //TODO get_node(ptr) -> Option<&Node>;
    fn get_node(&self, id: Id) -> Result<Rc<RefCell<Node>>, types::Error>;
    //TODO add_node(node) -> ptr
    fn add_node(&mut self, node: &Rc<RefCell<Node>>);
}

pub struct MockNodeStorage {
    nodes: HashMap<Id, Rc<RefCell<Node>>>,
}

impl MockNodeStorage {
    pub fn new() -> MockNodeStorage {
        MockNodeStorage {
            nodes: HashMap::new(),
        }
    }
}
impl NodeStorage for MockNodeStorage {
    fn get_node(&self, id: Id) -> Result<Rc<RefCell<Node>>, types::Error> {
        let r = self.nodes.get(&id);
        Ok(Rc::clone(r.unwrap()))
    }

    fn add_node(&mut self, node: &Rc<RefCell<Node>>) {
        self.nodes.insert(node.borrow().id, node.clone());
    }
}

pub fn scan<'a>(
    storage: &mut dyn NodeStorage,
    root: &Rc<RefCell<Node>>,
    key: i32,
) -> Result<Rc<RefCell<Node>>, types::Error> {
    let mut target = Rc::clone(root);

    loop {
        let mut node_id: i32 = -1;
        {
            let ref_target = target.borrow();
            if ref_target.is_leaf {
                return Ok(Rc::clone(&target));
            }
            let rec = ref_target.find(key);
            if rec.is_none() {
                break;
            }
            node_id = rec.unwrap().into_id();
        }
        let tmp = storage.get_node(node_id);
        match tmp {
            Ok(r) => {
                target = Rc::clone(&r);
            }
            Err(e) => {
                return Err(format!("{:?} not found - '{}'", node_id, e));
            }
        }
    }
    return Err("not found".to_owned());
}

pub fn find<'a>(
    storage: &mut dyn NodeStorage,
    root: &Rc<RefCell<Node>>,
    key: i32,
) -> Result<Record, types::Error> {
    let node = scan(storage, root, key);
    match node {
        Ok(n) => {
            let b = n.borrow();
            let r = b.find(key);
            return Ok(r.unwrap().clone());
        }
        Err(e) => Err(e),
    }
}

pub fn insert<'a>(
    storage: &mut dyn NodeStorage,
    root: &Rc<RefCell<Node>>,
    key: i32,
    value: &Record,
    t: i32,
) -> Result<Rc<RefCell<Node>>, types::Error> {
    let scan_result = scan(storage, &root, key);
    if scan_result.is_err() {
        return Err(scan_result.err().unwrap());
    }

    let target_node = scan_result.unwrap();

    todo!();
    return Ok(root.clone());
    //target_node.borrow_mut().data[0] = value.clone();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_in_leaf() {
        let leaf = Node::new_leaf(
            0,
            vec![1, 2, 3, 4],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
            ],
        );
        let ref_leaf = leaf.borrow();
        if let Some(item) = ref_leaf.find(2) {
            let v = item.into_u8();
            assert_eq!(v, 2u8);
        }

        if let Some(item) = ref_leaf.find(1) {
            let v = item.into_u8();
            assert_eq!(v, 1u8);
        }

        if let Some(item) = ref_leaf.find(4) {
            let v = item.into_u8();
            assert_eq!(v, 4u8);
        }

        let is_none = ref_leaf.find(9);
        assert_eq!(is_none, None);
    }

    #[test]
    fn find_in_tree() {
        let leaf1 = Node::new_leaf(0, vec![2, 3], vec![Record::from_u8(2), Record::from_u8(3)]);

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);
        let res = find(&mut storage, &leaf1, 2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().into_u8(), 2u8);

        let leaf2 = Node::new_leaf(1, vec![1], vec![Record::from_u8(1)]);
        storage.add_node(&leaf2);

        let node1 = Node::new_root(2, vec![2], vec![Record::from_id(1), Record::from_id(0)]);

        storage.add_node(&node1);
        let res_1 = find(&mut storage, &node1, 1);
        assert!(res_1.is_ok());
        assert_eq!(res_1.unwrap().into_u8(), 1u8);

        let res_2 = find(&mut storage, &node1, 2);
        assert!(res_2.is_ok());
        assert_eq!(res_2.unwrap().into_u8(), 2u8);
    }
    #[test]
    fn insert_to_tree() {
        let leaf1 = Node::new_leaf(0, vec![2, 3], vec![Record::from_u8(2), Record::from_u8(3)]);
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);

        let new_value = Record::from_u8(1);
        let new_root = insert(&mut storage, &leaf1, 1, &new_value, 4);
    }
}
