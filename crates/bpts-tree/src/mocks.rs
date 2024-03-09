use crate::{
    node::RcNode,
    nodestorage::NodeStorage,
    params::TreeParams,
    types::{self, Id},
    Result,
};
use std::{collections::HashMap, rc::Rc};

pub struct MockNodeStorage {
    nodes: HashMap<i32, RcNode>,
    params: TreeParams,
}

impl MockNodeStorage {
    pub fn size(&self) -> usize {
        return self.nodes.len();
    }
    pub fn new(params: TreeParams) -> MockNodeStorage {
        MockNodeStorage {
            nodes: HashMap::new(),
            params,
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

    pub fn change_t(&mut self, t: usize) {
        self.params.t = t;
        self.params.min_size_leaf = t;
        self.params.min_size_node = t;
        self.params.min_size_root = t;
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
    fn get_node(&self, id: Id) -> Result<RcNode> {
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

    fn get_params(&self) -> &TreeParams {
        &self.params
    }
}
