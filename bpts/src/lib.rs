pub mod mfile;
pub mod rec;
pub mod types;
pub mod utils;

use std::collections::HashMap;

use rec::Record;
use types::Id;

#[derive(Clone)]
pub struct Node {
    pub id: Id,
    pub is_leaf: bool,
    pub keys: Vec<i32>,
    pub data: Vec<Record>,
}

impl Node {
    pub fn new(id: Id, is_leaf: bool, keys: Vec<i32>, data: Vec<Record>) -> Node {
        Node {
            id: id,
            is_leaf: is_leaf,
            keys: keys,
            data: data,
        }
    }

    pub fn new_root(id: Id, keys: Vec<i32>, data: Vec<Record>) -> Node {
        Node::new(id, false, keys, data)
    }

    pub fn new_leaf(id: Id, keys: Vec<i32>, data: Vec<Record>) -> Node {
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
    fn get_node(&self, id: Id) -> Option<&Node>;
    fn add_node(&mut self, node: &Node);
}

pub struct MockNodeStorage {
    nodes: HashMap<Id, Node>,
}

impl MockNodeStorage {
    pub fn new() -> MockNodeStorage {
        MockNodeStorage {
            nodes: HashMap::new(),
        }
    }
}
impl NodeStorage for MockNodeStorage {
    fn get_node(&self, id: Id) -> Option<&Node> {
        self.nodes.get(&id)
    }

    fn add_node(&mut self, node: &Node) {
        self.nodes.insert(node.id, node.clone());
    }
}

pub fn scan<'a>(
    storage: &'a dyn NodeStorage,
    root: &'a Node,
    key: i32,
) -> Result<&'a Node, types::Error> {
    let mut target = root;

    loop {
        let rec = target.find(key);
        if target.is_leaf {
            return Ok(target);
        }
        if rec.is_none() {
            break;
        }
        let node_id = rec.unwrap().into_id();
        let tmp = storage.get_node(node_id);
        if tmp.is_none() {
            return Err(format!("{:?} not found", node_id));
        }
        target = tmp.unwrap();
    }
    return Err("not found".to_owned());
}

pub fn find<'a>(
    storage: &'a dyn NodeStorage,
    root: &'a Node,
    key: i32,
) -> Result<&'a Record, types::Error> {
    let node = scan(storage, root, key);
    match node {
        Ok(n) => Ok(n.find(key).unwrap()),
        Err(e) => Err(e),
    }
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
        if let Some(item) = leaf.find(2) {
            let v = item.into_u8();
            assert_eq!(v, 2u8);
        }

        if let Some(item) = leaf.find(1) {
            let v = item.into_u8();
            assert_eq!(v, 1u8);
        }

        if let Some(item) = leaf.find(4) {
            let v = item.into_u8();
            assert_eq!(v, 4u8);
        }

        let is_none = leaf.find(9);
        assert_eq!(is_none, None);
    }

    #[test]
    fn find_in_tree() {
        let leaf1 = Node::new_leaf(0, vec![2, 3], vec![Record::from_u8(2), Record::from_u8(3)]);

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);
        let res = find(&storage, &leaf1, 2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().into_u8(), 2u8);

        let leaf2 = Node::new_leaf(1, vec![1], vec![Record::from_u8(1)]);
        storage.add_node(&leaf2);

        let node1 = Node::new_root(2, vec![2], vec![Record::from_id(1), Record::from_id(0)]);

        storage.add_node(&node1);
        let res_1 = find(&storage, &node1, 1);
        assert!(res_1.is_ok());
        assert_eq!(res_1.unwrap().into_u8(), 1u8);

        let res_2 = find(&storage, &node1, 2);
        assert!(res_2.is_ok());
        assert_eq!(res_2.unwrap().into_u8(), 2u8);
    }
}
