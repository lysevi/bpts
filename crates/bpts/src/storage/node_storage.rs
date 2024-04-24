use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    tree::{
        node::{Node, NodeKeyCmp, RcNode},
        nodestorage::NodeStorage,
        record::Record,
        TreeParams,
    },
    types::Id,
    verbose, Result,
};

use super::{flat_storage::FlatStorage, KeyCmpRc, MAGIC_TRANSACTION, U32SZ, U8SZ};

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

    fn save_node(flat_store: &dyn FlatStorage, n: &Node) -> Result<()> {
        flat_store.write_id(n.id)?;
        flat_store.write_bool(n.is_leaf)?;
        flat_store.write_id(n.parent)?;
        flat_store.write_id(n.left)?;
        flat_store.write_id(n.right)?;
        flat_store.write_u32(n.keys_count as u32)?;
        flat_store.write_u32(n.data_count as u32)?;

        for k in n.key_iter() {
            flat_store.write_u32(*k)?;
        }

        for d in n.data_iter() {
            match *d {
                Record::Value(v) => flat_store.write_u32(v)?,
                Record::Ptr(ptr) => flat_store.write_id(ptr)?,
                Record::Empty => todo!(),
            }
        }
        Ok(())
    }

    pub(super) fn save(&mut self, tree_id: u32, flat_store: &dyn FlatStorage) -> Result<u32> {
        if self.offset != 0 {
            return Ok(self.offset);
        }
        let mut nodes_offsets = Vec::new();

        let mut new_offsets = HashMap::new();

        for node in self.nodes.values() {
            let node_ref = node.borrow();
            if let Some(exists_offset) = self.get_node_offset(node_ref.id) {
                nodes_offsets.push(exists_offset);
                continue;
            }
            let cur_write_offset = flat_store.size();

            nodes_offsets.push(cur_write_offset);
            new_offsets.insert(node_ref.id, cur_write_offset);

            Self::save_node(&*flat_store, &node_ref)?;
        }
        for o in new_offsets {
            self.set_node_offset(o.0, o.1);
        }

        self.offset = flat_store.size() as u32;
        flat_store.write_u32(MAGIC_TRANSACTION)?;
        flat_store.write_u32(tree_id)?;
        flat_store.write_u32(nodes_offsets.len() as u32)?;
        for i in nodes_offsets {
            flat_store.write_u32(i as u32)?;
        }
        Ok(self.offset)
    }

    fn load_node(&self, node_offset: u32, flat_store: &dyn FlatStorage) -> Result<RcNode> {
        let mut offset = node_offset as usize;
        let id = flat_store.read_id(offset)?;
        offset += U32SZ;
        let is_leaf = flat_store.read_bool(offset)?;
        offset += U8SZ;
        let parent = flat_store.read_id(offset)?;
        offset += U32SZ;
        let left = flat_store.read_id(offset)?;
        offset += U32SZ;
        let right = flat_store.read_id(offset)?;
        offset += U32SZ;
        let keys_count = flat_store.read_u32(offset)?;
        offset += U32SZ;
        let data_count = flat_store.read_u32(offset)?;
        offset += U32SZ;

        let mut keys = Vec::with_capacity(keys_count as usize);
        keys.resize(self.tree_params.get_keys_count(), 0u32);

        let mut data = Vec::with_capacity(keys_count as usize);
        data.resize(self.tree_params.get_keys_count(), Record::Empty);
        for i in 0..keys_count {
            let key = flat_store.read_u32(offset)?;
            offset += U32SZ;
            keys[i as usize] = key;
        }

        for i in 0..data_count {
            let d = flat_store.read_u32(offset)?;
            offset += U32SZ;
            data[i as usize] = if is_leaf {
                Record::Value(d)
            } else {
                Record::Ptr(Id(d))
            };
        }

        let node = Node::new_with_links(
            id,
            is_leaf,
            keys,
            data,
            keys_count as usize,
            data_count as usize,
            parent,
            left,
            right,
        );

        return Ok(node);
    }

    pub(super) fn load_trans(
        &mut self,
        start_offset: usize,
        flat_store: &dyn FlatStorage,
    ) -> Result<()> {
        let mut offset = start_offset;
        let count: u32 = flat_store.read_u32(offset)?;
        offset += U32SZ;
        let mut nodes_offsets = Vec::new();
        for _i in 0..count {
            let node_pos: u32 = flat_store.read_u32(offset)?;
            offset += U32SZ;
            nodes_offsets.push(node_pos);
        }

        for node_offset in nodes_offsets {
            let node = self.load_node(node_offset, flat_store)?;
            self.add_node_with_offset(&node, node_offset as usize);
        }
        Ok(())
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
