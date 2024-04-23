use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::tree::node::Node;
use crate::tree::nodestorage::NodeStorage;
use crate::tree::record::Record;
use crate::types::Id;
use crate::Result;

use super::cmp::StorageKeyCmpRef;
use super::cmp::StorageNodeCmp;
use super::flat_storage::FlatStorage;
use super::node_storage::{StorageNodeStorage, StorageNodeStorageRc};
use super::{KeyCmp, StorageParams};

/*
params:.... key+data.... [node] tree [links to node]  TRANSLIST [links to tree]
 */

const MAGIC_TRANSACTION: u32 = 0x66996699;
const MAGIC_TRANSACTION_LIST: u32 = 0xDDDBDDDB;

pub struct Storage {
    store: Rc<RefCell<dyn FlatStorage>>,
    params: StorageParams,
    cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    tree_storages: HashMap<u32, StorageNodeStorageRc>,
}

impl Storage {
    pub fn new(
        s: Rc<RefCell<dyn FlatStorage>>,
        params: &StorageParams,
        cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    ) -> Result<Self> {
        s.borrow_mut().header_write(&params)?;

        Ok(Storage {
            store: s,
            params: params.clone(),
            cmp: cmp,
            tree_storages: HashMap::new(),
        })
    }

    pub fn open(
        s: Rc<RefCell<dyn FlatStorage>>,
        cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    ) -> Result<Self> {
        let params = s.borrow().header_read()?;
        Ok(Storage {
            store: s,
            cmp: cmp,
            params: params,
            tree_storages: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        self.params.is_closed = 1;
        self.store.borrow_mut().header_write(&self.params)?;
        Ok(())
    }

    pub(super) fn insert_kv(store: &dyn FlatStorage, key: &[u8], data: &[u8]) -> Result<u32> {
        //TODO Result<u32> => Result<u64>
        let offset = store.size();
        store.write_u32(key.len() as u32)?;
        for i in key.iter() {
            store.write_u8(*i)?;
        }
        store.write_u32(data.len() as u32)?;
        for i in data.iter() {
            store.write_u8(*i)?;
        }
        return Ok(offset as u32);
    }

    pub(super) fn read_kv(store: &dyn FlatStorage, offset: usize) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut read_offset = offset;
        let key_len = store.read_u32(read_offset)?;
        read_offset += std::mem::size_of::<u32>();
        let mut key = Vec::new();
        let mut data = Vec::new();
        for _i in 0..key_len {
            let val = store.read_u8(read_offset)?;
            read_offset += std::mem::size_of::<u8>();
            key.push(val);
        }

        let data_len = store.read_u32(read_offset)?;
        read_offset += std::mem::size_of::<u32>();
        for _i in 0..data_len {
            let val = store.read_u8(read_offset)?;
            read_offset += std::mem::size_of::<u8>();
            data.push(val);
        }
        return Ok((key, data));
    }

    fn get_tree_cmp(&self, tree_id: u32) -> Rc<RefCell<StorageNodeCmp>> {
        let cmp = Rc::new(RefCell::new(StorageNodeCmp {
            store: self.store.clone(),
            cmp: self.cmp.get(&tree_id).unwrap().clone(),
        }));
        return cmp;
    }

    fn make_cmp(&self, tree_id: u32, key: &[u8]) -> Rc<RefCell<StorageKeyCmpRef>> {
        let cmp = Rc::new(RefCell::new(StorageKeyCmpRef {
            store: self.store.clone(),
            user_key: key.to_vec(),
            cmp: self.cmp.get(&tree_id).unwrap().clone(),
        }));
        return cmp;
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        let tparams = self.params.tree_params.clone();

        let key_offset = Self::insert_kv(&*self.store.borrow_mut(), key, data)?;
        {
            let target_storage = if let Some(t) = self.tree_storages.get(&tree_id) {
                let c = t.clone();
                c.borrow_mut().offset = 0;
                c
            } else {
                let s = Rc::new(RefCell::new(StorageNodeStorage {
                    offset: 0,
                    cmp: Some(self.get_tree_cmp(tree_id)),
                    nodes: HashMap::new(),
                    nodes_to_offset: HashMap::new(),
                    tree_params: self.params.tree_params,
                }));

                self.tree_storages.insert(tree_id, s.clone());
                s.clone()
            };
            target_storage
                .borrow_mut()
                .set_cmp(self.get_tree_cmp(tree_id));
            let mut storage_ref = (*target_storage).borrow_mut();

            let root = if let Some(t) = storage_ref.get_root() {
                t.clone()
            } else {
                let root_node = Node::new_leaf_with_size(Id(tree_id), tparams.t);
                storage_ref.add_node(&root_node);
                root_node
            };

            let _insert_res = crate::tree::insert::insert(
                &mut *storage_ref,
                &root,
                key_offset,
                &crate::tree::record::Record::from_u32(key_offset),
            )?;
        }
        self.save_trans()?;

        Ok(())
    }

    fn save_trans(&mut self) -> Result<()> {
        let mut trans_list = Vec::new();
        let flat_store = self.store.borrow_mut();

        for ns in self.tree_storages.iter() {
            if ns.1.borrow().offset != 0 {
                trans_list.push(ns.1.borrow().offset);
                continue;
            }
            let mut nodes_offsets = Vec::new();
            {
                let mut cur_store = ns.1.borrow_mut();
                let mut new_offsets = HashMap::new();

                for node in cur_store.nodes.values() {
                    if let Some(exists_offset) = cur_store.get_node_offset(node.borrow().id) {
                        nodes_offsets.push(exists_offset);
                        continue;
                    }
                    let cur_node_offset = flat_store.size();
                    let node_ref = node.borrow();
                    nodes_offsets.push(cur_node_offset);
                    new_offsets.insert(node.borrow().id, cur_node_offset);
                    flat_store.write_id(node_ref.id)?;
                    flat_store.write_bool(node_ref.is_leaf)?;
                    flat_store.write_id(node_ref.parent)?;
                    flat_store.write_id(node_ref.left)?;
                    flat_store.write_id(node_ref.right)?;
                    flat_store.write_u32(node_ref.keys_count as u32)?;
                    flat_store.write_u32(node_ref.data_count as u32)?;
                    for k in node_ref.key_iter() {
                        flat_store.write_u32(*k)?;
                    }
                    for d in node_ref.data_iter() {
                        match *d {
                            Record::Value(v) => flat_store.write_u32(v)?,
                            Record::Ptr(ptr) => flat_store.write_id(ptr)?,
                            Record::Empty => todo!(),
                        }
                    }
                }
                for o in new_offsets {
                    cur_store.set_node_offset(o.0, o.1);
                }
            }

            ns.1.borrow_mut().offset = flat_store.size() as u32;
            trans_list.push(flat_store.size() as u32);
            flat_store.write_u32(MAGIC_TRANSACTION)?;
            flat_store.write_u32(*ns.0)?;
            flat_store.write_u32(nodes_offsets.len() as u32)?;
            for i in nodes_offsets {
                flat_store.write_u32(i as u32)?;
            }
        }

        self.params.offset = flat_store.size() as u32;

        flat_store.write_u32(MAGIC_TRANSACTION_LIST)?;
        flat_store.write_u32(trans_list.len() as u32)?;
        for i in trans_list {
            flat_store.write_u32(i)?;
        }

        flat_store.header_write(&self.params)?;
        Ok(())
    }

    fn load_trans(&mut self) -> Result<()> {
        self.tree_storages.clear();
        let store = self.store.borrow();
        let hdr = store.header_read()?;
        if hdr.offset == 0 {
            panic!();
        }

        let mut trees_offsets = Vec::new();
        {
            let mut offset = hdr.offset as usize;
            let magic_lst = store.read_u32(offset)?;
            offset += std::mem::size_of::<u32>();
            if magic_lst != MAGIC_TRANSACTION_LIST {
                panic!();
            }
            let storages_count = store.read_u32(offset)?;
            offset += std::mem::size_of::<u32>();

            for _i in 0..storages_count {
                let v = store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();
                trees_offsets.push(v);
            }
        }

        for start in trees_offsets {
            let mut offset = start as usize;
            let magic = store.read_u32(offset)?;
            if magic != MAGIC_TRANSACTION {
                panic!();
            }
            offset += std::mem::size_of::<u32>();

            let tree_id = store.read_u32(offset)?;

            let s = Rc::new(RefCell::new(StorageNodeStorage {
                offset: offset as u32,
                cmp: Some(self.get_tree_cmp(tree_id)),
                nodes: HashMap::new(),
                nodes_to_offset: HashMap::new(),
                tree_params: self.params.tree_params,
            }));
            self.tree_storages.insert(tree_id, s.clone());

            offset += std::mem::size_of::<u32>();
            let count: u32 = store.read_u32(offset)?;
            offset += std::mem::size_of::<u32>();
            let mut nodes_offsets = Vec::new();
            for _i in 0..count {
                let node_pos: u32 = store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();
                nodes_offsets.push(node_pos);
            }
            for node_offset in nodes_offsets {
                offset = node_offset as usize;
                let id = store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let is_leaf = store.read_bool(offset)?;
                offset += std::mem::size_of::<u8>();
                let parent = store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let left = store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let right = store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let keys_count = store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();
                let data_count = store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();

                let mut keys = Vec::with_capacity(keys_count as usize);
                keys.resize(self.params.tree_params.get_keys_count(), 0u32);

                let mut data = Vec::with_capacity(keys_count as usize);
                data.resize(self.params.tree_params.get_keys_count(), Record::Empty);
                for i in 0..keys_count {
                    let key = store.read_u32(offset)?;
                    offset += std::mem::size_of::<u32>();
                    keys[i as usize] = key;
                }

                for i in 0..data_count {
                    let d = store.read_u32(offset)?;
                    offset += std::mem::size_of::<u32>();
                    data[i as usize] = if is_leaf {
                        Record::Value(d)
                    } else {
                        Record::Ptr(Id(d))
                    };
                }
                let node = Node::new(
                    id,
                    is_leaf,
                    keys,
                    data,
                    keys_count as usize,
                    data_count as usize,
                );
                node.borrow_mut().parent = parent;
                node.borrow_mut().left = left;
                node.borrow_mut().right = right;
                s.borrow_mut()
                    .add_node_with_offset(&node, node_offset as usize);
            }
        }
        Ok(())
    }

    fn get_storage_for_tree(
        &self,
        tree_id: u32,
    ) -> Result<Option<Rc<RefCell<StorageNodeStorage>>>> {
        let storage_res = self.tree_storages.get(&tree_id);
        if storage_res.is_none() {
            return Ok(None);
        }
        return Ok(Some(storage_res.unwrap().clone()));
    }

    pub fn find(&mut self, tree_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.load_trans()?;

        let storage = if let Some(x) = self.get_storage_for_tree(tree_id)? {
            x.borrow_mut().set_cmp(self.make_cmp(tree_id, key));
            x
        } else {
            return Ok(None);
        };

        let root = storage.borrow().get_root();
        if root.is_none() {
            return Ok(None);
        }

        let mut a = storage.borrow_mut();
        let find_res = crate::tree::read::find(&mut *a, &root.unwrap().clone(), std::u32::MAX)?;
        if find_res.is_none() {
            return Ok(None);
        }

        let offset = find_res.unwrap().into_u32();
        let kv = Self::read_kv(&*self.store.borrow(), offset as usize)?;
        return Ok(Some(kv.1));
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{types::SingleElementStore, utils::any_as_u8_slice, Result};

    struct MockStorageKeyCmp {}

    impl MockStorageKeyCmp {
        fn new() -> MockStorageKeyCmp {
            MockStorageKeyCmp {}
        }
    }

    impl KeyCmp for MockStorageKeyCmp {
        fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
            key1.cmp(key2)
        }
    }

    struct MockPageStorage {
        hdr: RefCell<SingleElementStore<StorageParams>>,
        space: RefCell<Vec<u8>>,
    }

    impl MockPageStorage {
        pub fn new() -> MockPageStorage {
            MockPageStorage {
                hdr: RefCell::new(SingleElementStore::new()),
                space: RefCell::new(Vec::new()),
            }
        }

        pub fn size(&self) -> usize {
            self.space.borrow().len()
        }
    }

    impl FlatStorage for MockPageStorage {
        fn header_write(&self, h: &StorageParams) -> Result<()> {
            self.hdr.borrow_mut().replace(h.clone());
            Ok(())
        }

        fn header_read(&self) -> Result<StorageParams> {
            if !self.hdr.borrow().is_empty() {
                let rf = self.hdr.borrow_mut();
                let value = rf.as_value();
                return Ok(value);
            }
            panic!();
        }

        fn size(&self) -> usize {
            self.space.borrow_mut().len()
        }

        fn write_id(&self, v: Id) -> Result<()> {
            return self.write_u32(v.0);
        }

        fn write_bool(&self, v: bool) -> Result<()> {
            if v {
                self.space.borrow_mut().push(1u8)
            } else {
                self.space.borrow_mut().push(0u8)
            }
            Ok(())
        }

        fn write_u8(&self, v: u8) -> Result<()> {
            self.space.borrow_mut().push(v);
            Ok(())
        }

        fn write_u16(&self, v: u16) -> Result<()> {
            let sl = unsafe { any_as_u8_slice(&v) };
            for i in sl.iter() {
                self.write_u8(*i)?;
            }
            Ok(())
        }

        fn write_u32(&self, v: u32) -> Result<()> {
            let sl = unsafe { any_as_u8_slice(&v) };
            for i in sl.iter() {
                self.write_u8(*i)?;
            }
            Ok(())
        }

        fn write_u64(&self, v: u64) -> Result<()> {
            let sl = unsafe { any_as_u8_slice(&v) };
            for i in sl.iter() {
                self.write_u8(*i)?;
            }
            Ok(())
        }

        fn read_id(&self, seek: usize) -> Result<Id> {
            let v = self.read_u32(seek)?;
            Ok(Id(v))
        }

        fn read_bool(&self, seek: usize) -> Result<bool> {
            let v = self.read_u8(seek)?;
            return Ok(if v == 1 { true } else { false });
        }

        fn read_u8(&self, seek: usize) -> Result<u8> {
            Ok(self.space.borrow()[seek])
        }

        fn read_u16(&self, seek: usize) -> Result<u16> {
            let readed = unsafe { (self.space.borrow().as_ptr().add(seek) as *const u16).read() };
            Ok(readed)
        }

        fn read_u32(&self, seek: usize) -> Result<u32> {
            let readed = unsafe { (self.space.borrow().as_ptr().add(seek) as *const u32).read() };
            Ok(readed)
        }

        fn read_u64(&self, seek: usize) -> Result<u64> {
            let readed = unsafe { (self.space.borrow().as_ptr().add(seek) as *const u64).read() };
            Ok(readed)
        }
    }

    #[test]
    fn db() -> Result<()> {
        let mut all_cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>> = HashMap::new();
        let cmp = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp);

        let fstore = Rc::new(RefCell::new(MockPageStorage::new()));
        let params = StorageParams::default();
        let mut storage = Storage::new(fstore.clone(), &params, all_cmp)?;
        let max_key = 400;
        for key in 0..max_key {
            if key == 200 {
                println!("");
            };
            println!("insert {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            storage.insert(1, &cur_key_sl, &cur_key_sl)?;
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, cur_key_sl)
            }

            for search_key in 0..key {
                println!("read {}", search_key);
                let key_sl = unsafe { any_as_u8_slice(&search_key) };
                let find_res = storage.find(1, key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, key_sl)
            }
        }

        for key in 0..max_key {
            println!("read {}", key);
            let key_sl = unsafe { any_as_u8_slice(&key) };
            let find_res = storage.find(1, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        storage.close()?;
        println!("size: {}kb", fstore.borrow().size() as f32 / 1024f32);
        Ok(())
    }
}
