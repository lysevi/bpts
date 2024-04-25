use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
};

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
    pub(super) nodes: RefCell<HashMap<u32, RcNode>>,
    pub(super) nodes_to_offset: RefCell<HashMap<u32, usize>>,
    pub tree_params: TreeParams,
    flat_store: Rc<RefCell<dyn FlatStorage>>,
}

impl StorageNodeStorage {
    pub(super) fn new(
        offset: u32,
        cmp: KeyCmpRc,
        flat_store: Rc<RefCell<dyn FlatStorage>>,
        params: TreeParams,
    ) -> Rc<RefCell<StorageNodeStorage>> {
        Rc::new(RefCell::new(StorageNodeStorage {
            offset: offset as u32,
            cmp: Some(cmp),
            nodes: RefCell::new(HashMap::new()),
            nodes_to_offset: RefCell::new(HashMap::new()),
            tree_params: params,
            flat_store: flat_store,
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

    // pub(super) fn add_node_with_offset(&self, node: &RcNode, offset: usize) {
    //     let ref_node = node.borrow();
    //     self.nodes
    //         .borrow_mut()
    //         .insert(ref_node.id.unwrap(), node.clone());
    //     self.nodes_to_offset
    //         .borrow_mut()
    //         .insert(node.borrow().id.0, offset);
    // }

    pub(super) fn get_node_offset(&self, id: Id) -> Option<usize> {
        if let Some(v) = self.nodes_to_offset.borrow().get(&id.0) {
            return Some(*v);
        }
        return None;
    }

    pub(super) fn set_node_offset(&mut self, id: Id, offset: usize) {
        self.nodes_to_offset.borrow_mut().insert(id.0, offset);
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

        let mut loaded_nodes = HashSet::new();
        let mut all_nodes = Vec::with_capacity(self.nodes.borrow().len());
        for i in self.nodes.borrow().values() {
            loaded_nodes.insert(i.borrow().id.0);
            if i.borrow().parent.is_empty() {
                if all_nodes.len() == 0 {
                    all_nodes.push(i.clone())
                } else {
                    let firts = all_nodes[0].clone();
                    all_nodes[0] = i.clone();
                    all_nodes.push(firts);
                }
            } else {
                all_nodes.push(i.clone());
            }
        }
        assert!(self.get_root().unwrap().borrow().id == all_nodes[0].borrow().id);
        for node in all_nodes {
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
        for i in self.nodes_to_offset.borrow().iter() {
            if !loaded_nodes.contains(i.0) {
                nodes_offsets.push(*i.1);
            }
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

    fn load_node_header(&mut self, node_offset: u32) -> Result<()> {
        let offset = node_offset as usize;
        let id = self.flat_store.borrow().read_id(offset)?;
        self.nodes_to_offset
            .borrow_mut()
            .insert(id.0, node_offset as usize);
        Ok(())
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

    pub(super) fn load_trans(&mut self, start_offset: usize) -> Result<()> {
        let mut nodes_offsets = Vec::new();
        {
            let fstore_ref = self.flat_store.borrow();
            let mut offset = start_offset;
            let count: u32 = fstore_ref.read_u32(offset)?;
            offset += U32SZ;

            for _i in 0..count {
                let node_pos: u32 = fstore_ref.read_u32(offset)?;
                offset += U32SZ;
                nodes_offsets.push(node_pos);
            }
        }
        let mut is_first = true;
        for node_offset in nodes_offsets {
            self.load_node_header(node_offset)?;
            if is_first {
                let node = self.load_node(node_offset, &*self.flat_store.borrow())?;
                self.nodes
                    .borrow_mut()
                    .insert(node.borrow().id.0, node.clone());
                is_first = false;
            }
        }
        Ok(())
    }
}

impl NodeStorage for StorageNodeStorage {
    fn get_root(&self) -> Option<RcNode> {
        let nodes_ref = self.nodes.borrow();
        if nodes_ref.len() == 1 {
            let res = nodes_ref.iter().next();
            let res = res.unwrap();
            let res = res.1;
            return Some(res.clone());
        }
        for i in nodes_ref.iter() {
            let node = i.1;
            if !node.borrow().is_leaf && node.borrow().parent.is_empty() {
                return Some(node.clone());
            }
        }
        None
    }
    fn get_new_id(&self) -> Id {
        let nodes = self.nodes.borrow();
        let max_id1 = nodes
            .keys()
            .into_iter()
            .max_by(|x, y| x.cmp(y))
            .unwrap_or(&0u32);

        let nodes_offsets = self.nodes_to_offset.borrow();
        let max_id2 = nodes_offsets
            .keys()
            .into_iter()
            .max_by(|x, y| x.cmp(y))
            .unwrap_or(&0u32);

        return Id(std::cmp::max(*max_id1, *max_id2) + 1);
    }

    fn get_node(&self, id: Id) -> crate::Result<RcNode> {
        verbose!("get_node {:?}", id);

        {
            let nodes = self.nodes.borrow();
            let res = nodes.get(&id.unwrap());
            if let Some(r) = res {
                return Ok(r.clone());
            }
        }
        {
            if let Some(node_offset) = self.nodes_to_offset.borrow().get(&id.0) {
                let node = self.load_node(*node_offset as u32, &*self.flat_store.borrow())?;
                self.nodes.borrow_mut().insert(id.0, node.clone());
                Ok(node)
            } else {
                Err(crate::Error::Fail(format!("not found Id={}", id.0)))
            }
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        let ref_node = node.borrow();
        self.nodes
            .borrow_mut()
            .insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        verbose!("erase_node {:?}", id);
        self.nodes.borrow_mut().remove(&id.0);
    }

    fn get_params(&self) -> &TreeParams {
        &self.tree_params
    }

    fn get_cmp(&self) -> &dyn NodeKeyCmp {
        self
    }

    fn mark_as_changed(&mut self, id: Id) {
        verbose!("mark_as_changed {:?}", id);
        self.nodes_to_offset.borrow_mut().remove(&id.0);
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
