use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::tree::node::Node;
use crate::tree::nodestorage::NodeStorage;
use crate::tree::TreeParams;
use crate::types::Id;
use crate::Result;

use super::cmp::StorageKeyCmpRef;
use super::cmp::StorageNodeCmp;
use super::flat_storage::FlatStorage;
use super::node_storage::{StorageNodeStorage, StorageNodeStorageRc};
use super::MAGIC_HEADER;
use super::MAGIC_TRANSACTION;
use super::MAGIC_TRANSACTION_LIST;
use super::U32SZ;
use super::U8SZ;
use super::{KeyCmp, StorageParams};

/*
params:.... key+data.... [node] tree [links to node]  TRANSLIST [links to tree]
 */

#[derive(Clone, Copy)]
pub struct StorageHeader {
    magic: u32,
    offset: u32,
    is_closed: u8,
}

pub struct Storage {
    store: Rc<RefCell<dyn FlatStorage>>,
    params: StorageParams,
    header: StorageHeader,
    cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    tree_storages: HashMap<u32, StorageNodeStorageRc>,
}

impl Storage {
    pub fn new(
        s: Rc<RefCell<dyn FlatStorage>>,
        params: &StorageParams,
        cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    ) -> Result<Self> {
        let p = params.clone();
        s.borrow_mut().params_write(&p)?;

        let h = StorageHeader {
            magic: MAGIC_HEADER,
            is_closed: 0,
            offset: 0,
        };
        s.borrow_mut().header_write(&h)?;
        Ok(Storage {
            store: s,
            params: p,
            header: h,
            cmp: cmp,
            tree_storages: HashMap::new(),
        })
    }

    pub fn open(
        s: Rc<RefCell<dyn FlatStorage>>,
        cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>>,
    ) -> Result<Self> {
        let params = s.borrow().params_read()?;
        let header = s.borrow().header_read()?;

        if header.magic != MAGIC_HEADER {
            todo!("fsck")
        }

        Ok(Storage {
            store: s,
            cmp: cmp,
            params: params,
            header: header,
            tree_storages: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        self.header.is_closed = 1;
        self.store.borrow_mut().header_write(&self.header)?;
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

    // pub(super) fn read_kv(store: &dyn FlatStorage, offset: usize) -> Result<(Vec<u8>, Vec<u8>)> {
    //     let mut read_offset = offset;
    //     let key_len = store.read_u32(read_offset)?;
    //     read_offset += U32SZ;
    //     let mut key = Vec::new();
    //     let mut data = Vec::new();
    //     for _i in 0..key_len {
    //         let val = store.read_u8(read_offset)?;
    //         read_offset += U8SZ;
    //         key.push(val);
    //     }

    //     let data_len = store.read_u32(read_offset)?;
    //     read_offset += U32SZ;
    //     for _i in 0..data_len {
    //         let val = store.read_u8(read_offset)?;
    //         read_offset += U8SZ;
    //         data.push(val);
    //     }
    //     return Ok((key, data));
    // }

    pub(super) fn read_key(store: &dyn FlatStorage, offset: usize) -> Result<Vec<u8>> {
        let mut read_offset = offset;
        let key_len = store.read_u32(read_offset)?;
        read_offset += U32SZ;
        let mut key = Vec::new();
        for _i in 0..key_len {
            let val = store.read_u8(read_offset)?;
            read_offset += U8SZ;
            key.push(val);
        }

        return Ok(key);
    }

    pub(super) fn read_kdata(store: &dyn FlatStorage, offset: usize) -> Result<Vec<u8>> {
        let mut read_offset = offset;
        let key_len = store.read_u32(read_offset)?;
        read_offset += U32SZ;

        read_offset += U8SZ * key_len as usize;

        let data_len = store.read_u32(read_offset)?;
        let mut data = Vec::with_capacity(data_len as usize);
        read_offset += U32SZ;
        for _i in 0..data_len {
            let val = store.read_u8(read_offset)?;
            read_offset += U8SZ;
            data.push(val);
        }
        return Ok(data);
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

    pub fn dump_tree(&self, tree_id: u32, name: String) -> String {
        let storage = self.tree_storages.get(&tree_id).unwrap();
        let root = storage.borrow().get_root().unwrap();
        return crate::tree::debug::storage_to_string(
            &*storage.borrow(),
            root.clone(),
            true,
            &name,
        );
    }

    fn save_trees(&mut self) -> Result<()> {
        let mut trans_list = Vec::new();
        let flat_store = self.store.borrow_mut();

        for ns in self.tree_storages.iter() {
            let mut cur_store = ns.1.borrow_mut();
            let cur_store_offset = cur_store.save(*ns.0, &*flat_store)?;
            trans_list.push(cur_store_offset);
        }

        self.header.offset = flat_store.size() as u32;

        flat_store.write_u32(MAGIC_TRANSACTION_LIST)?;
        flat_store.write_u32(trans_list.len() as u32)?;
        for i in trans_list {
            flat_store.write_u32(i)?;
        }

        flat_store.header_write(&self.header)?;
        Ok(())
    }

    fn load_trees(&mut self) -> Result<()> {
        self.tree_storages.clear();
        let store = self.store.borrow();
        let hdr = store.header_read()?;
        if hdr.offset == 0 {
            panic!();
        }

        let mut trees_offsets = Vec::new();
        // loading tree offsets
        {
            let mut offset = hdr.offset as usize;
            let magic_lst = store.read_u32(offset)?;
            offset += U32SZ;
            if magic_lst != MAGIC_TRANSACTION_LIST {
                panic!();
            }
            let storages_count = store.read_u32(offset)?;
            offset += U32SZ;

            for _i in 0..storages_count {
                let v = store.read_u32(offset)?;
                offset += U32SZ;
                trees_offsets.push(v);
            }
        }

        // tree loading.
        for start in trees_offsets {
            let mut offset = start as usize;
            let magic = store.read_u32(offset)?;
            offset += U32SZ;

            if magic != MAGIC_TRANSACTION {
                panic!();
            }

            let tree_id = store.read_u32(offset)?;

            let s = StorageNodeStorage::new(
                start as u32,
                self.get_tree_cmp(tree_id),
                self.store.clone(),
                self.params.tree_params,
            );
            self.tree_storages.insert(tree_id, s.clone());

            s.borrow_mut().load(start as usize)?;
        }
        Ok(())
    }

    fn get_exist_storage_for_tree(
        &self,
        tree_id: u32,
    ) -> Result<Option<Rc<RefCell<StorageNodeStorage>>>> {
        let storage_res = self.tree_storages.get(&tree_id);
        if storage_res.is_none() {
            return Ok(None);
        }
        return Ok(Some(storage_res.unwrap().clone()));
    }

    fn get_or_create_storage_for_tree(
        &mut self,
        tree_id: u32,
    ) -> Result<Rc<RefCell<StorageNodeStorage>>> {
        let target_storage = if let Some(t) = self.tree_storages.get(&tree_id) {
            let c = t.clone();
            c.borrow_mut()
                .set_offset(0)
                .set_cmp(self.get_tree_cmp(tree_id));
            c
        } else {
            let s = StorageNodeStorage::new(
                0u32,
                self.get_tree_cmp(tree_id),
                self.store.clone(),
                self.params.tree_params,
            );

            self.tree_storages.insert(tree_id, s.clone());
            s.clone()
        };
        Ok(target_storage)
    }

    fn insert_to_tree(&mut self, tree_id: u32, key_offset: u32, tparams: TreeParams) -> Result<()> {
        let target_storage = self.get_or_create_storage_for_tree(tree_id)?;

        let mut storage_ref = (*target_storage).borrow_mut();

        let root = if let Some(t) = storage_ref.get_root() {
            t.clone()
        } else {
            let root_node = Node::new_leaf_with_size(Id(1), tparams.t);
            storage_ref.add_node(&root_node);
            root_node
        };

        let _insert_res = crate::tree::insert::insert(
            &mut *storage_ref,
            &root,
            key_offset,
            &crate::tree::record::Record::from_u32(key_offset),
        )?;
        Ok(())
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        let tparams = self.params.tree_params.clone();

        let key_offset = Self::insert_kv(&*self.store.borrow_mut(), key, data)?;
        self.insert_to_tree(tree_id, key_offset, tparams)?;
        self.save_trees()?;

        Ok(())
    }

    pub fn find(&mut self, tree_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.load_trees()?;

        let storage = if let Some(x) = self.get_exist_storage_for_tree(tree_id)? {
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
        let d = Self::read_kdata(&*self.store.borrow(), offset as usize)?;
        return Ok(Some(d));
    }

    pub fn remove(&mut self, tree_id: u32, key: &[u8]) -> Result<()> {
        self.load_trees()?;

        if let Some(t) = self.tree_storages.get(&tree_id) {
            let storage = t.clone();
            storage
                .borrow_mut()
                .set_offset(0)
                .set_cmp(self.make_cmp(tree_id, key));
            let root = storage.borrow().get_root();
            if root.is_none() {
                return Ok(());
            }

            let mut a = storage.borrow_mut();
            crate::tree::remove::remove_key(&mut *a, &root.unwrap().clone(), std::u32::MAX)?;
        } else {
            return Ok(());
        };

        self.save_trees()?;
        Ok(())
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
        hdr: RefCell<SingleElementStore<StorageHeader>>,
        params: RefCell<SingleElementStore<StorageParams>>,
        space: RefCell<Vec<u8>>,
    }

    impl MockPageStorage {
        pub fn new() -> MockPageStorage {
            MockPageStorage {
                params: RefCell::new(SingleElementStore::new()),
                hdr: RefCell::new(SingleElementStore::new()),
                space: RefCell::new(Vec::with_capacity(1024 * 1024 * 5)),
            }
        }

        pub fn size(&self) -> usize {
            self.space.borrow().len()
        }
    }

    impl FlatStorage for MockPageStorage {
        fn params_write(&self, h: &StorageParams) -> Result<()> {
            self.params.borrow_mut().replace(h.clone());
            Ok(())
        }

        fn params_read(&self) -> Result<StorageParams> {
            if !self.params.borrow().is_empty() {
                let rf = self.params.borrow_mut();
                let value = rf.as_value();
                return Ok(value);
            }
            panic!();
        }

        fn header_write(&self, h: &StorageHeader) -> Result<()> {
            self.hdr.borrow_mut().replace(h.clone());
            Ok(())
        }

        fn header_read(&self) -> Result<StorageHeader> {
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
        let mut all_keys = Vec::new();
        for key in 0..max_key {
            //println!("insert {}", key);

            all_keys.push(key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            storage.insert(1, &cur_key_sl, &cur_key_sl)?;
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, cur_key_sl)
            }

            // for search_key in 0..key {
            //     println!("read {}", search_key);
            //     let key_sl = unsafe { any_as_u8_slice(&search_key) };
            //     let find_res = storage.find(1, key_sl)?;
            //     assert!(find_res.is_some());
            //     let value = &find_res.unwrap()[..];
            //     assert_eq!(value, key_sl)
            // }
        }

        for key in all_keys.iter() {
            //println!("read {}", key);
            let key_sl = unsafe { any_as_u8_slice(key) };
            let find_res = storage.find(1, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        while all_keys.len() > 0 {
            let key = all_keys[0];
            all_keys.remove(0);

            //println!("remove {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            let str_before = storage.dump_tree(1, String::from("before"));
            storage.remove(1, &cur_key_sl)?;
            let str_after = storage.dump_tree(1, String::from("after"));

            //crate::tree::debug::print_states(&[&str_before, &str_after]);
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_none());
            }

            for search_key in all_keys.iter() {
                //println!("read {}", search_key);
                let key_sl = unsafe { any_as_u8_slice(search_key) };
                let find_res = storage.find(1, key_sl)?;
                if find_res.is_none() {
                    crate::tree::debug::print_states(&[&str_before, &str_after]);
                }
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, key_sl)
            }
        }

        let mut hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 0);
        storage.close()?;
        hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 1);
        println!("size: {}kb", fstore.borrow().size() as f32 / 1024f32);
        Ok(())
    }

    #[test]
    fn db_rev() -> Result<()> {
        let mut all_cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>> = HashMap::new();
        let cmp = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp);

        let fstore = Rc::new(RefCell::new(MockPageStorage::new()));
        let params = StorageParams::default();
        let mut storage = Storage::new(fstore.clone(), &params, all_cmp)?;
        let max_key = 400;
        let mut all_keys = Vec::new();
        for key in 0..max_key {
            //println!("insert {}", key);
            all_keys.push(key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            storage.insert(1, &cur_key_sl, &cur_key_sl)?;
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, cur_key_sl)
            }
        }

        for key in all_keys.iter() {
            // println!("read {}", key);
            let key_sl = unsafe { any_as_u8_slice(key) };
            let find_res = storage.find(1, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        while all_keys.len() > 0 {
            let last = all_keys.len() - 1;
            let key = all_keys[last];
            all_keys.remove(last);

            //println!("remove {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            let str_before = storage.dump_tree(1, String::from("before"));
            storage.remove(1, &cur_key_sl)?;
            let str_after = storage.dump_tree(1, String::from("after"));

            //crate::tree::debug::print_states(&[&str_before, &str_after]);
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_none());
            }

            for search_key in all_keys.iter() {
                //println!("read {}", search_key);
                let key_sl = unsafe { any_as_u8_slice(search_key) };
                let find_res = storage.find(1, key_sl)?;
                if find_res.is_none() {
                    crate::tree::debug::print_states(&[&str_before, &str_after]);
                }
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, key_sl)
            }
        }

        let mut hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 0);
        storage.close()?;
        hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 1);
        println!("size: {}kb", fstore.borrow().size() as f32 / 1024f32);
        Ok(())
    }

    #[test]
    fn db_many_trees() -> Result<()> {
        let mut all_cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>> = HashMap::new();
        let cmp1 = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        let cmp2 = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp1);
        all_cmp.insert(2u32, cmp2);

        let fstore = Rc::new(RefCell::new(MockPageStorage::new()));
        let params = StorageParams::default();
        let mut storage = Storage::new(fstore.clone(), &params, all_cmp)?;
        let max_key = 400;
        let mut all_keys = Vec::new();
        for key in 0..max_key {
            //println!("insert {}", key);
            let tree_id = if key % 2 == 0 { 1 } else { 2 };

            all_keys.push(key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            storage.insert(tree_id, &cur_key_sl, &cur_key_sl)?;
            {
                let find_res = storage.find(tree_id, cur_key_sl)?;
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, cur_key_sl)
            }

            // for search_key in 0..key {
            //     println!("read {}", search_key);
            //     let key_sl = unsafe { any_as_u8_slice(&search_key) };
            //     let find_res = storage.find(1, key_sl)?;
            //     assert!(find_res.is_some());
            //     let value = &find_res.unwrap()[..];
            //     assert_eq!(value, key_sl)
            // }
        }

        for key in all_keys.iter() {
            //println!("read {}", key);
            let search_tree_id = if key % 2 == 0 { 1 } else { 2 };
            let key_sl = unsafe { any_as_u8_slice(key) };
            let find_res = storage.find(search_tree_id, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        while all_keys.len() > 0 {
            let key = all_keys[0];
            all_keys.remove(0);
            let tree_id = if key % 2 == 0 { 1 } else { 2 };
            // println!("remove {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            let str_before = storage.dump_tree(tree_id, String::from("before"));
            storage.remove(tree_id, &cur_key_sl)?;
            let str_after = storage.dump_tree(tree_id, String::from("after"));

            //crate::tree::debug::print_states(&[&str_before, &str_after]);
            {
                let find_res = storage.find(tree_id, cur_key_sl)?;
                assert!(find_res.is_none());
            }

            for search_key in all_keys.iter() {
                //println!("read {}", search_key);
                let search_tree_id = if search_key % 2 == 0 { 1 } else { 2 };
                let key_sl = unsafe { any_as_u8_slice(search_key) };
                let find_res = storage.find(search_tree_id, key_sl)?;
                if find_res.is_none() {
                    crate::tree::debug::print_states(&[&str_before, &str_after]);
                }
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, key_sl)
            }
        }

        let mut hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 0);
        storage.close()?;
        hdr = fstore.borrow().header_read()?;
        assert!(hdr.is_closed == 1);
        println!("size: {}kb", fstore.borrow().size() as f32 / 1024f32);
        Ok(())
    }
}
