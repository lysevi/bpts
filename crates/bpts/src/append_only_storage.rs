use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::tree::node::{KeyCmp, Node, RcNode};
use crate::tree::nodestorage::NodeStorage;
use crate::tree::params::TreeParams;
use crate::tree::record::Record;
use crate::types::Id;
use crate::Result;

/*
params:.... MAGIC_NUMBERkey+data...transaction...MAGIC_NUMBERtransaction_list
transaction_list - set links to trees.
 */

const MAGIC_TRANSACTION: u32 = 0x66996699;
const MAGIC_TRANSACTION_LIST: u32 = 0xDDDBDDDB;

pub trait AppendOnlyStruct {
    fn header_write(&self, h: &AOStorageParams) -> Result<()>;
    fn header_read(&self) -> Result<AOStorageParams>;

    fn size(&self) -> usize;
    fn write_id(&self, v: Id) -> Result<()>;
    fn write_bool(&self, v: bool) -> Result<()>;
    fn write_u8(&self, v: u8) -> Result<()>;
    fn write_u16(&self, v: u16) -> Result<()>;
    fn write_u32(&self, v: u32) -> Result<()>;
    fn write_u64(&self, v: u64) -> Result<()>;

    fn read_id(&self, seek: usize) -> Result<Id>;
    fn read_bool(&self, seek: usize) -> Result<bool>;
    fn read_u8(&self, seek: usize) -> Result<u8>;
    fn read_u16(&self, seek: usize) -> Result<u16>;
    fn read_u32(&self, seek: usize) -> Result<u32>;
    fn read_u64(&self, seek: usize) -> Result<u64>;
}

pub trait AOSKeyCmp {
    fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering;
}

#[derive(Clone, Copy)]
pub struct AOStorageParams {
    offset: u32,
    tree_params: TreeParams,
}

pub struct AOStorageNodeStorage<Cmp: KeyCmp> {
    cmp: Cmp,
    pub nodes: HashMap<u32, RcNode>,
    pub tree_params: TreeParams,
}

impl AOStorageParams {
    pub fn default() -> Self {
        Self {
            offset: 0,
            tree_params: TreeParams::default(),
        }
    }
}

pub struct AOStorageCmp<'a, Store: AppendOnlyStruct> {
    store: &'a Store,
}

impl<'a, Store: AppendOnlyStruct> KeyCmp for AOStorageCmp<'a, Store> {
    fn compare(&self, _key1: u32, _key2: u32) -> std::cmp::Ordering {
        todo!()
    }
}

pub struct AOStorage<'a, Store: AppendOnlyStruct> {
    store: &'a Store,
    params: AOStorageParams,
    cmp: &'a HashMap<u32, Rc<RefCell<dyn AOSKeyCmp>>>,
    tree_storages: HashMap<u32, Rc<RefCell<AOStorageNodeStorage<AOStorageCmp<'a, Store>>>>>,
}

impl<Cmp: KeyCmp> NodeStorage for AOStorageNodeStorage<Cmp> {
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
        self.nodes.remove(&id.0);
    }

    fn get_params(&self) -> &TreeParams {
        &self.tree_params
    }

    fn get_cmp(&self) -> &dyn KeyCmp {
        &self.cmp
    }
}

impl<'a, Store: AppendOnlyStruct> AOStorage<'a, Store> {
    pub fn new(
        s: &'a Store,
        params: &AOStorageParams,
        cmp: &'a HashMap<u32, Rc<RefCell<dyn AOSKeyCmp>>>,
    ) -> Result<Self> {
        s.header_write(&params)?;

        Ok(AOStorage {
            store: s,
            params: params.clone(),
            cmp: cmp,
            tree_storages: HashMap::new(),
        })
    }

    pub fn open(s: &'a Store, cmp: &'a HashMap<u32, Rc<RefCell<dyn AOSKeyCmp>>>) -> Result<Self> {
        let params = s.header_read()?;
        Ok(AOStorage {
            store: s,
            cmp: cmp,
            params: params,
            tree_storages: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        todo!();
    }

    fn insert_kv(&self, key: &[u8], data: &[u8]) -> Result<u32> {
        //TODO Result<u32> => Result<u64>
        let offset = self.store.size();
        self.store.write_u32(key.len() as u32)?;
        for i in key.iter() {
            self.store.write_u8(*i)?;
        }
        self.store.write_u32(data.len() as u32)?;
        for i in data.iter() {
            self.store.write_u8(*i)?;
        }
        return Ok(offset as u32);
    }

    fn read_kv(&self, offset: usize) -> Result<(Vec<u8>, Vec<u8>)> {
        //TODO Result<u32> => Result<u64>

        let mut read_offset = offset;
        let key_len = self.store.read_u32(read_offset)?;
        read_offset += std::mem::size_of::<u32>();
        let mut key = Vec::new();
        let mut data = Vec::new();
        for _i in 0..key_len {
            let val = self.store.read_u8(read_offset)?;
            read_offset += std::mem::size_of::<u8>();
            key.push(val);
        }

        let data_len = self.store.read_u32(read_offset)?;
        read_offset += std::mem::size_of::<u32>();
        for _i in 0..data_len {
            let val = self.store.read_u8(read_offset)?;
            read_offset += std::mem::size_of::<u8>();
            data.push(val);
        }
        return Ok((key, data));
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        let tparams = self.params.tree_params.clone();

        let key_offset = self.insert_kv(key, data)?;
        {
            let target_storage = if let Some(t) = self.tree_storages.get(&tree_id) {
                t.clone()
            } else {
                let cmp = AOStorageCmp { store: self.store };
                let s = Rc::new(RefCell::new(AOStorageNodeStorage {
                    cmp: cmp,
                    nodes: HashMap::new(),
                    tree_params: self.params.tree_params,
                }));

                self.tree_storages.insert(tree_id, s.clone());
                s.clone()
            };

            let mut storage_ref = (*target_storage).borrow_mut();

            let root = if let Some(t) = storage_ref.nodes.get(&tree_id) {
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

        let offset = self.store.size();
        self.store.write_u32(MAGIC_TRANSACTION_LIST)?;
        self.store.write_u32(self.tree_storages.len() as u32)?;
        for ns in self.tree_storages.iter() {
            self.store.write_u32(MAGIC_TRANSACTION)?;
            self.store.write_u32(*ns.0)?;
            let cur_store = ns.1.borrow();
            self.store.write_u32(cur_store.nodes.len() as u32)?;

            for node in cur_store.nodes.values() {
                let node_ref = node.borrow();
                self.store.write_id(node_ref.id)?;
                self.store.write_bool(node_ref.is_leaf)?;
                self.store.write_id(node_ref.parent)?;
                self.store.write_id(node_ref.left)?;
                self.store.write_id(node_ref.right)?;
                self.store.write_u32(node_ref.keys_count as u32)?;
                self.store.write_u32(node_ref.data_count as u32)?;
                for k in node_ref.key_iter() {
                    self.store.write_u32(*k)?;
                }
                for d in node_ref.data_iter() {
                    match *d {
                        Record::Value(v) => self.store.write_u32(v)?,
                        Record::Ptr(ptr) => self.store.write_id(ptr)?,
                        Record::Empty => todo!(),
                    }
                }
            }
            self.params.offset = offset as u32;
            self.store.header_write(&self.params)?;
        }
        Ok(())
    }

    fn load_trans(&mut self) -> Result<()> {
        self.tree_storages.clear();

        let hdr = self.store.header_read()?;
        if hdr.offset == 0 {
            panic!();
        }

        let mut offset = hdr.offset as usize;
        let magic_lst = self.store.read_u32(offset)?;
        offset += std::mem::size_of::<u32>();
        if magic_lst != MAGIC_TRANSACTION_LIST {
            panic!();
        }
        let storages_count = self.store.read_u32(offset)?;
        offset += std::mem::size_of::<u32>();
        for _tree_store in 0..storages_count {
            let magic = self.store.read_u32(offset)?;
            if magic != MAGIC_TRANSACTION {
                panic!();
            }
            offset += std::mem::size_of::<u32>();

            let tree_id = self.store.read_u32(offset)?;
            let cmp = AOStorageCmp { store: self.store };
            let s = Rc::new(RefCell::new(AOStorageNodeStorage {
                cmp: cmp,
                nodes: HashMap::new(),
                tree_params: self.params.tree_params,
            }));
            self.tree_storages.insert(tree_id, s.clone());

            offset += std::mem::size_of::<u32>();
            let count: u32 = self.store.read_u32(offset)?;
            offset += std::mem::size_of::<u32>();
            for i in 0..count {
                let id = self.store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let is_leaf = self.store.read_bool(offset)?;
                offset += std::mem::size_of::<u8>();
                let parent = self.store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let left = self.store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let right = self.store.read_id(offset)?;
                offset += std::mem::size_of::<u32>();
                let keys_count = self.store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();
                let data_count = self.store.read_u32(offset)?;
                offset += std::mem::size_of::<u32>();
                let mut keys = Vec::with_capacity(keys_count as usize);
                let mut data = Vec::with_capacity(keys_count as usize);
                for _i in 0..keys_count {
                    let key = self.store.read_u32(offset)?;
                    offset += std::mem::size_of::<u32>();
                    keys.push(key);
                }

                for _i in 0..data_count {
                    let d = self.store.read_u32(offset)?;
                    offset += std::mem::size_of::<u32>();
                    if is_leaf {
                        data.push(Record::Value(d));
                    } else {
                        data.push(Record::Ptr(Id(d)));
                    }
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
                s.borrow_mut().add_node(&node);
                //self.nodes.insert(id.0, node);
            }
        }
        Ok(())
    }

    pub fn find(&mut self, tree_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.load_trans()?;
        let storage_res = self.tree_storages.get(&tree_id);
        if storage_res.is_none() {
            return Ok(None);
        }
        let storage = storage_res.unwrap().clone();
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
        let kv = self.read_kv(offset as usize)?;
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

    impl AOSKeyCmp for MockStorageKeyCmp {
        fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
            key1.cmp(key2)
        }
    }

    struct MockPageStorage {
        hdr: RefCell<SingleElementStore<AOStorageParams>>,
        space: RefCell<Vec<u8>>,
    }

    impl MockPageStorage {
        pub fn new() -> MockPageStorage {
            MockPageStorage {
                hdr: RefCell::new(SingleElementStore::new()),
                space: RefCell::new(Vec::new()),
            }
        }
    }

    impl AppendOnlyStruct for MockPageStorage {
        fn header_write(&self, h: &AOStorageParams) -> Result<()> {
            self.hdr.borrow_mut().replace(h.clone());
            Ok(())
        }

        fn header_read(&self) -> Result<AOStorageParams> {
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
        let mut all_cmp: HashMap<u32, Rc<RefCell<dyn AOSKeyCmp>>> = HashMap::new();
        let cmp = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp);

        let fstore = MockPageStorage::new();
        let params = AOStorageParams::default();
        let mut storage = AOStorage::new(&fstore, &params, &all_cmp)?;
        let max_key = 400;
        for key in 0..max_key {
            println!("insert {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            storage.insert(1, &cur_key_sl, &cur_key_sl)?;
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, cur_key_sl)
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
        Ok(())
    }
}
