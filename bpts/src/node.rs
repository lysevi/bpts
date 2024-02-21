use std::{cell::RefCell, rc::Rc};

use crate::rec;
use crate::types;
use crate::utils;

use rec::Record;
use types::Id;

pub type RcNode = Rc<RefCell<Node>>;

#[derive(Clone)]
pub struct Node {
    pub id: Id, //TODO remove
    pub is_leaf: bool,
    pub parent: Id,
    pub left: Id,
    pub right: Id,
    pub keys_count: usize,
    pub data_count: usize,
    pub keys: Vec<i32>,
    pub data: Vec<Record>,
}

impl Node {
    pub fn new(
        id: Id,
        is_leaf: bool,
        keys: Vec<i32>,
        data: Vec<Record>,
        keys_count: usize,
        data_count: usize,
    ) -> RcNode {
        Rc::new(RefCell::new(Node {
            id: id,
            is_leaf: is_leaf,
            keys: keys,
            data: data,
            keys_count,
            data_count,
            left: types::EMPTY_ID,
            parent: types::EMPTY_ID,
            right: types::EMPTY_ID,
        }))
    }

    pub fn new_root(
        id: Id,
        keys: Vec<i32>,
        data: Vec<Record>,
        keys_count: usize,
        data_count: usize,
    ) -> RcNode {
        Node::new(id, false, keys, data, keys_count, data_count)
    }

    pub fn new_leaf(
        id: Id,
        keys: Vec<i32>,
        data: Vec<Record>,
        keys_count: usize,
        data_count: usize,
    ) -> RcNode {
        Node::new(id, true, keys, data, keys_count, data_count)
    }

    pub fn can_insert(&self, t: usize) -> bool {
        return self.data_count < (2 * t - 1);
    }

    pub fn is_empty(&self) -> bool {
        return self.keys_count == 0;
    }

    pub fn find(&self, key: i32) -> Option<&Record> {
        if key < self.keys[0] {
            return self.data.first();
        }

        if !self.is_leaf {
            if self.keys[self.keys_count - 1] <= key {
                return Some(&self.data[self.data_count - 1]);
            }

            for i in 0..self.keys_count {
                match (self.keys[i]).cmp(&key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => return Some(&self.data[i + 1]),
                    std::cmp::Ordering::Greater => return Some(&self.data[i]),
                }
            }
            return None;
        }

        for i in 0..self.keys_count {
            match (self.keys[i]).cmp(&key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Some(&self.data[i]),
                std::cmp::Ordering::Greater => return Some(&self.data[i]),
            }
        }
        return None;
    }

    pub fn map<'a, F>(&self, from: i32, to: i32, f: &mut F)
    where
        F: FnMut(i32, &Record),
    {
        if !self.is_leaf {
            panic!()
        }

        for i in 0..self.keys_count {
            if self.keys[i] >= from && self.keys[i] <= to {
                f(self.keys[i], &self.data[i]);
            }
        }
    }

    pub fn insert_data(&mut self, index: usize, key: i32, value: rec::Record) {
        utils::insert_to_array(&mut self.keys, index, key);
        utils::insert_to_array(&mut self.data, index, value);
        self.keys_count += 1;
        self.data_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_in_leaf() {
        let leaf = Node::new_leaf(
            types::EMPTY_ID,
            vec![1, 2, 3, 4],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
            ],
            4,
            4,
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
    fn find_in_midle() {
        let leaf = Node::new_root(
            types::EMPTY_ID,
            vec![3, 5, 7],
            vec![
                Record::from_u8(1),
                Record::from_u8(3),
                Record::from_u8(5),
                Record::from_u8(7),
            ],
            3,
            4,
        );
        let ref_leaf = leaf.borrow();
        if let Some(item) = ref_leaf.find(1) {
            let v = item.into_u8();
            assert_eq!(v, 1u8);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(3) {
            let v = item.into_u8();
            assert_eq!(v, 3u8);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(4) {
            let v = item.into_u8();
            assert_eq!(v, 3u8);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(9) {
            let v = item.into_u8();
            assert_eq!(v, 7u8);
        } else {
            assert!(false);
        }
    }
}
