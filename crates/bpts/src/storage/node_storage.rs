use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    tree::{
        node::{NodeKeyCmp, RcNode},
        nodestorage::NodeStorage,
        TreeParams,
    },
    types::Id,
    verbose,
};

use super::KeyCmpRc;

pub(super) type StorageNodeStorageRc = Rc<RefCell<StorageNodeStorage>>;

pub struct StorageNodeStorage {
    pub(super) offset: u32,
    pub(super) cmp: Option<KeyCmpRc>,
    pub(super) nodes: HashMap<u32, RcNode>,
    pub(super) nodes_to_offset: HashMap<u32, usize>,
    pub tree_params: TreeParams,
}

impl StorageNodeStorage {
    pub(super) fn new(
        offset: u32,
        cmp: KeyCmpRc,
        params: TreeParams,
    ) -> Rc<RefCell<StorageNodeStorage>> {
        Rc::new(RefCell::new(StorageNodeStorage {
            offset: offset as u32,
            cmp: Some(cmp),
            nodes: HashMap::new(),
            nodes_to_offset: HashMap::new(),
            tree_params: params,
        }))
    }
    pub(super) fn set_cmp(&mut self, c: KeyCmpRc) -> &mut Self {
        self.cmp = Some(c);
        self
    }

    pub(super) fn set_offset(&mut self, v: u32) -> &mut Self {
        self.offset = v;
        self
    }

    pub(super) fn add_node_with_offset(&mut self, node: &RcNode, offset: usize) {
        self.add_node(node);
        self.nodes_to_offset.insert(node.borrow().id.0, offset);
    }

    pub(super) fn get_node_offset(&self, id: Id) -> Option<usize> {
        if let Some(v) = self.nodes_to_offset.get(&id.0) {
            return Some(*v);
        }
        return None;
    }

    pub(super) fn set_node_offset(&mut self, id: Id, offset: usize) {
        self.nodes_to_offset.insert(id.0, offset);
    }
}

impl NodeStorage for StorageNodeStorage {
    fn get_root(&self) -> Option<RcNode> {
        if self.nodes.len() == 1 {
            let res = self.nodes.iter().next();
            let res = res.unwrap();
            let res = res.1;
            return Some(res.clone());
        }
        for i in &self.nodes {
            let node = i.1;
            if !node.borrow().is_leaf && node.borrow().parent.is_empty() {
                return Some(node.clone());
            }
        }
        None
    }
    fn get_new_id(&self) -> Id {
        let max = self.nodes.keys().into_iter().max_by(|x, y| x.cmp(y));
        match max {
            Some(x) => {
                let n = x + 1;
                Id(n)
            }
            None => Id(1),
        }
    }

    fn get_node(&self, id: Id) -> crate::Result<RcNode> {
        verbose!("get_node {:?}", id);
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(r.clone())
        } else {
            Err(crate::Error::Fail(format!("not found Id={}", id.0)))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        verbose!("erase_node {:?}", id);
        self.nodes.remove(&id.0);
    }

    fn get_params(&self) -> &TreeParams {
        &self.tree_params
    }

    fn get_cmp(&self) -> &dyn NodeKeyCmp {
        self
    }

    fn mark_as_changed(&mut self, id: Id) {
        verbose!("mark_as_changed {:?}", id);
        self.nodes_to_offset.remove(&id.0);
    }
}

impl NodeKeyCmp for StorageNodeStorage {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        match &self.cmp {
            Some(c) => {
                let r = c.borrow();
                return r.compare(key1, key2);
            }
            None => panic!(),
        }
    }
}
