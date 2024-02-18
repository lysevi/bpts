use std::{collections::HashMap, rc::Rc};

use crate::{
    node::RcNode,
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
    fn get_node(&self, id: &Id) -> Result<RcNode, types::Error> {
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(Rc::clone(r))
        } else {
            Err(types::Error("not found".to_owned()))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }
}
