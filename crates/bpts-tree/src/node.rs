use std::{cell::RefCell, rc::Rc};

use crate::rec;
use crate::types;
use crate::utils;

use rec::Record;
use types::Id;

pub type RcNode = Rc<RefCell<Node>>;

#[derive(Clone)]
pub struct Node {
    pub id: Id,
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
            left: Id::empty(),
            parent: Id::empty(),
            right: Id::empty(),
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

    pub fn new_leaf_with_size(id: Id, t: usize) -> RcNode {
        let mut keys = Vec::with_capacity(t * 2);
        let mut recs = Vec::with_capacity(t * 2);
        for _i in 0..(t * 2) {
            recs.push(Record::Empty);
            keys.push(0i32);
        }
        Node::new(id, true, keys, recs, 0, 0)
    }

    pub fn can_insert(&self, t: usize) -> bool {
        return self.data_count < (2 * t - 1);
    }

    pub fn is_empty(&self) -> bool {
        return self.keys_count == 0;
    }

    pub fn find_key(&self, key: i32) -> Option<i32> {
        if self.is_leaf {
            panic!("logic error");
        }
        if key < self.keys[0] {
            return Some(*self.keys.first().unwrap());
        }

        if self.keys[self.keys_count - 1] <= key {
            return Some(self.keys[self.keys_count - 1]);
        }

        for i in 0..self.keys_count {
            match (self.keys[i]).cmp(&key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Some(self.keys[i]),
                std::cmp::Ordering::Greater => return Some(self.keys[i - 1]),
            }
        }
        return None;
    }

    pub fn find(&self, key: i32) -> Option<Record> {
        if !self.is_leaf {
            if key < self.keys[0] {
                return Some(self.data.first().unwrap().clone());
            }

            if self.keys[self.keys_count - 1] <= key {
                return Some(self.data[self.data_count - 1].clone());
            }

            for i in 0..self.keys_count {
                match (self.keys[i]).cmp(&key) {
                    std::cmp::Ordering::Less => continue,
                    std::cmp::Ordering::Equal => return Some(self.data[i + 1].clone()),
                    std::cmp::Ordering::Greater => return Some(self.data[i].clone()),
                }
            }
            return None;
        }

        for i in 0..self.keys_count {
            match (self.keys[i]).cmp(&key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Some(self.data[i].clone()),
                std::cmp::Ordering::Greater => continue, //return Some(self.data[i].clone()),
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

    pub fn map_rev<'a, F>(&self, from: i32, to: i32, f: &mut F)
    where
        F: FnMut(i32, &Record),
    {
        if !self.is_leaf {
            panic!()
        }

        for i in (0..self.keys_count).rev() {
            let cur_key = self.keys[i];
            if cur_key >= from && cur_key <= to {
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

    pub fn update_key(&mut self, child: Id, new_key: i32) {
        println!(
            "update key target={:?} child={:?} new={}",
            self.id, child, new_key
        );

        if self.is_leaf {
            panic!("logic error");
        } else {
            if self.data[0].into_id() == child {
                return;
            }

            if self.data[self.data_count - 1].into_id() == child {
                self.keys[self.keys_count - 1] = new_key;
                return;
            }

            for i in 1..self.data_count {
                if self.data[i].into_id() == child {
                    self.keys[i - 1] = new_key;
                    return;
                }
            }
        }
    }

    pub fn erase_link(&mut self, child: Id) {
        println!("erase_link target={:?} child={:?}", self.id, child);

        if self.is_leaf {
            panic!("logic error");
        } else {
            if self.data[0].into_id() == child {
                utils::remove_with_shift(&mut self.keys, 0);
                self.keys_count -= 1;
                utils::remove_with_shift(&mut self.data, 0);
                self.data_count -= 1;
                return;
            }

            if self.data[self.data_count - 1].into_id() == child {
                utils::remove_with_shift(&mut self.keys, self.keys_count - 1);
                self.keys_count -= 1;
                utils::remove_with_shift(&mut self.data, self.data_count - 1);
                self.data_count -= 1;
                return;
            }

            for i in 0..self.data_count {
                if self.data[i].into_id() == child {
                    if i != 0 {
                        utils::remove_with_shift(&mut self.keys, i - 1);
                        self.keys_count -= 1;
                    }
                    utils::remove_with_shift(&mut self.data, i);
                    self.data_count -= 1;
                    return;
                }
            }

            panic!("not found");
        }
    }

    pub fn first_key(&self) -> i32 {
        if self.keys_count > 0 {
            return self.keys[0];
        }
        panic!("empty node");
    }

    pub fn first_data(&self) -> Record {
        if self.data_count > 0 {
            return self.data[0].clone();
        }
        panic!("empty node");
    }

    pub fn last_key(&self) -> i32 {
        if self.keys_count > 0 {
            return self.keys[self.keys_count - 1];
        }
        panic!("empty node");
    }

    pub fn last_data(&self) -> Record {
        if self.data_count > 0 {
            return self.data[self.data_count - 1].clone();
        }
        panic!("empty node");
    }

    pub fn data_iter(&self) -> std::iter::Take<std::slice::Iter<'_, Record>> {
        self.data.iter().take(self.data_count)
    }

    pub fn key_iter(&self) -> std::iter::Take<std::slice::Iter<'_, i32>> {
        self.keys.iter().take(self.keys_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_in_leaf() {
        let leaf = Node::new_leaf(
            Id::empty(),
            vec![1, 2, 3, 4],
            vec![
                Record::from_i32(1),
                Record::from_i32(2),
                Record::from_i32(3),
                Record::from_i32(4),
            ],
            4,
            4,
        );
        let ref_leaf = leaf.borrow();
        if let Some(item) = ref_leaf.find(2) {
            let v = item.into_i32();
            assert_eq!(v, 2);
        }

        if let Some(item) = ref_leaf.find(1) {
            let v = item.into_i32();
            assert_eq!(v, 1);
        }

        if let Some(item) = ref_leaf.find(4) {
            let v = item.into_i32();
            assert_eq!(v, 4);
        }

        let is_none = ref_leaf.find(9);
        assert_eq!(is_none, None);
    }

    #[test]
    fn find_in_midle() {
        let leaf = Node::new_root(
            Id::empty(),
            vec![3, 5, 7],
            vec![
                Record::from_i32(1),
                Record::from_i32(3),
                Record::from_i32(5),
                Record::from_i32(7),
            ],
            3,
            4,
        );
        let ref_leaf = leaf.borrow();
        if let Some(item) = ref_leaf.find(1) {
            let v = item.into_i32();
            assert_eq!(v, 1);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(3) {
            let v = item.into_i32();
            assert_eq!(v, 3);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(4) {
            let v = item.into_i32();
            assert_eq!(v, 3);
        } else {
            assert!(false);
        }

        if let Some(item) = ref_leaf.find(9) {
            let v = item.into_i32();
            assert_eq!(v, 7);
        } else {
            assert!(false);
        }
    }
}
