pub mod mfile;
pub mod node;
pub mod rec;
pub mod types;
pub mod utils;

use std::{collections::HashMap, rc::Rc};

use node::*;
use rec::Record;
use types::Id;

pub trait NodeStorage {
    fn get_new_id(&self) -> i32;
    //TODO get_node(ptr) -> Option<&Node>;
    fn get_node(&self, id: Id) -> Result<RcNode, types::Error>;
    //TODO add_node(node) -> ptr
    fn add_node(&mut self, node: &RcNode);
}

pub fn scan<'a>(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
) -> Result<RcNode, types::Error> {
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
    root: &RcNode,
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

pub fn split_node(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    t: usize,
) -> Result<RcNode, types::Error> {
    let mut parent_node: RcNode;
    let mut ref_root = root.borrow_mut();
    let is_new_root;
    if ref_root.parent == 0 || ref_root.is_leaf {
        // create new_root
        is_new_root = true;
        let new_data = Record::empty_array(ref_root.data.len(), ref_root.data[0].size());
        parent_node = Node::new_root(
            storage.get_new_id(),
            vec![0i32; ref_root.keys.capacity()],
            new_data,
            0,
            0,
        );
        storage.add_node(&parent_node);
    } else {
        //TODO! check unwrap
        is_new_root = false;
        parent_node = storage.get_node(ref_root.parent).unwrap();
    }

    let new_brother: RcNode;
    let new_id = storage.get_new_id();
    let mut new_keys = vec![0i32; ref_root.keys.capacity()];
    let mut new_data = Record::empty_array(ref_root.data.len(), ref_root.data[0].size());

    let mut keys_count = 0;
    let mut data_count = 0;
    {
        let midle_index = (ref_root.keys_count / 2);
        let last_index = ref_root.keys_count;
        for i in midle_index..last_index {
            new_keys[keys_count] = ref_root.keys[i];
            ref_root.keys[i] = 0;
            keys_count += 1;
        }
    }
    {
        let midle_index = (ref_root.data_count / 2);
        let last_index = ref_root.data_count;
        for i in midle_index..last_index {
            new_data[data_count] = ref_root.data[i].clone();
            data_count += 1
        }
    }

    if ref_root.is_leaf {
        new_brother = Node::new_leaf(new_id, new_keys, new_data, keys_count, data_count)
    } else {
        new_brother = Node::new_root(new_id, new_keys, new_data, keys_count, data_count)
    }

    //TODO! check result
    storage.add_node(&new_brother);

    let lowest_key = new_brother.borrow().keys[0];
    if is_new_root {
        let mut ref_to_parent = parent_node.borrow_mut();
        ref_to_parent.keys[0] = lowest_key;
        ref_to_parent.keys_count = 1;

        ref_to_parent.data[0] = Record::from_id(ref_root.id);
        ref_to_parent.data[1] = Record::from_id(new_brother.borrow().id);
        ref_to_parent.data_count = 2;

        return Ok(parent_node.clone());
    } else {
        return insert(
            storage,
            &parent_node,
            lowest_key,
            &Record::from_id(new_id),
            t,
        );
    }
}

pub fn insert(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
    value: &Record,
    t: usize,
) -> Result<RcNode, types::Error> {
    {
        //TODO! extract method
        let target_node: RcNode;
        if root.borrow().is_empty() {
            target_node = root.clone();
        } else {
            let scan_result = scan(storage, &root, key);
            if scan_result.is_err() {
                return Err(scan_result.err().unwrap());
            }

            target_node = scan_result.unwrap();
        }
        let mut mut_ref = target_node.borrow_mut();
        let can_insert = mut_ref.can_insert(t);

        let mut index = mut_ref.keys_count;
        for i in 0..mut_ref.keys_count {
            if mut_ref.keys[i] > key {
                index = i;
                break;
            }

            if mut_ref.keys[i] == 0 {
                index = i;
                break;
            }
        }
        mut_ref.insert_data(index, key, value.clone());

        if can_insert {
            return Ok(root.clone());
        }
    }
    return split_node(storage, root, t);
}

#[cfg(test)]
mod tests {
    use super::*;

    pub struct MockNodeStorage {
        nodes: HashMap<Id, RcNode>,
    }

    impl MockNodeStorage {
        pub fn new() -> MockNodeStorage {
            MockNodeStorage {
                nodes: HashMap::new(),
            }
        }
    }
    impl NodeStorage for MockNodeStorage {
        fn get_new_id(&self) -> i32 {
            let max = self.nodes.keys().into_iter().max_by(|x, y| x.cmp(y));
            match max {
                Some(x) => x + 1,
                None => 1,
            }
        }
        fn get_node(&self, id: Id) -> Result<RcNode, types::Error> {
            let r = self.nodes.get(&id);
            Ok(Rc::clone(r.unwrap()))
        }

        fn add_node(&mut self, node: &RcNode) {
            let ref_node = node.borrow();
            self.nodes.insert(ref_node.id, node.clone());
        }
    }

    #[test]
    fn find_in_tree() {
        let leaf1 = Node::new_leaf(
            0,
            vec![2, 3],
            vec![Record::from_u8(2), Record::from_u8(3)],
            2,
            2,
        );

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);
        let res = find(&mut storage, &leaf1, 2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().into_u8(), 2u8);

        let leaf2 = Node::new_leaf(1, vec![1], vec![Record::from_u8(1)], 1, 1);
        storage.add_node(&leaf2);

        let node1 = Node::new_root(
            2,
            vec![2],
            vec![Record::from_id(1), Record::from_id(0)],
            1,
            2,
        );

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
        let mut leaf1 = Node::new_leaf(
            1,
            vec![2, 3, 0, 0, 0, 0, 0],
            vec![
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(0),
                Record::from_u8(0),
                Record::from_u8(0),
                Record::from_u8(0),
                Record::from_u8(0),
            ],
            2,
            2,
        );

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);

        let new_value = Record::from_u8(1);
        let mut result = insert(&mut storage, &leaf1, 1, &new_value, 3);
        assert!(result.is_ok());
        let mut new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 3);

        result = insert(&mut storage, &leaf1, 5, &new_value, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 4);

        result = insert(&mut storage, &leaf1, 4, &new_value, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert_eq!(new_root.borrow().keys_count, 5);

        {
            let r = new_root.borrow();
            for i in 0..r.keys_count {
                assert_eq!(r.keys[i], (i + 1) as i32)
            }
        }

        result = insert(&mut storage, &leaf1, 6, &new_value, 3);
        assert!(result.is_ok());
        new_root = result.unwrap();
        assert!(!new_root.borrow().is_leaf);
        let search_result = find(&mut storage, &new_root, 6);
        assert!(search_result.is_ok());
    }
}
