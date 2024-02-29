use std::{collections::HashMap, rc::Rc};

use crate::{
    node::{self, RcNode},
    nodestorage::NodeStorage,
    types::{self, Id},
};

pub struct MockNodeStorage {
    nodes: HashMap<i32, RcNode>,
}

impl MockNodeStorage {
    pub fn size(&self) -> usize {
        return self.nodes.len();
    }
    pub fn new() -> MockNodeStorage {
        MockNodeStorage {
            nodes: HashMap::new(),
        }
    }

    pub fn is_exists(&self, id: Id) -> bool {
        self.nodes.contains_key(&id.0)
    }

    pub fn all<F>(&self, f: F) -> bool
    where
        F: FnMut(&RcNode) -> bool,
    {
        self.nodes.values().all(f)
    }

    pub fn print(&self, root: RcNode) {
        let mut to_print = Vec::new();
        to_print.push(root.clone());

        while !to_print.is_empty() {
            let mut children = Vec::new();
            for item in &to_print {
                let r_ref = item.borrow();
                MockNodeStorage::print_node(&r_ref);
                print!("  ");

                if !r_ref.is_leaf {
                    let data = &r_ref.data[0..r_ref.data_count];
                    for id in data.into_iter().map(|x| x.into_id()) {
                        children.push(self.get_node(id).unwrap());
                    }
                }
            }
            to_print = children;
            println!("");
        }
    }

    fn print_node(node: &node::Node) {
        let key_slice = &node.keys[0..node.keys_count];
        let string_data = if node.is_leaf {
            let unpack: Vec<u8> = node
                .data
                .iter()
                .take(node.data_count)
                .map(|f| f.into_u8())
                .collect();
            format!("{:?}", unpack)
        } else {
            let unpack: Vec<types::Id> = node
                .data
                .iter()
                .take(node.data_count)
                .map(|f| f.into_id())
                .collect();
            format!("{:?}", unpack)
        };
        let left = if node.left.exists() {
            format!("{}", node.left.0)
        } else {
            "_".to_owned()
        };

        let right = if node.right.exists() {
            format!("{}", node.right.0)
        } else {
            "_".to_owned()
        };

        let up = if node.parent.exists() {
            format!("{}", node.parent.0)
        } else {
            "_".to_owned()
        };
        let is_leaf_sfx = if node.is_leaf {
            " ".to_owned()
        } else {
            "*".to_owned()
        };
        print!(
            "Id:{:?}{}({},{},{})  <{:?}->{}>",
            node.id.0, is_leaf_sfx, left, right, up, key_slice, string_data
        );
    }
}
impl NodeStorage for MockNodeStorage {
    fn get_new_id(&self) -> types::Id {
        let max = self.nodes.keys().into_iter().max_by(|x, y| x.cmp(y));
        match max {
            Some(x) => {
                let n = x + 1;
                types::Id(n)
            }
            None => types::Id(1),
        }
    }
    fn get_node(&self, id: Id) -> Result<RcNode, types::Error> {
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(Rc::clone(r))
        } else {
            Err(types::Error(format!("not found Id={}", id.0)))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        println!("erase node: Id={}", id.0);
        self.nodes.remove(&id.0);
    }
}
