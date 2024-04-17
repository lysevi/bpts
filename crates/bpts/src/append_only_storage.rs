use std::collections::HashMap;
use std::fs::File;

use crate::page::PageKeyCmpRc;
use crate::transaction::Transaction;
use crate::tree::params::{self, TreeParams};
use crate::Result;

/*
params:.... MAGIC_NUMBERkey+data...transaction...MAGIC_NUMBERtransaction_list
transaction_list - set links to trees.
 */

pub trait AppendOnlyStruct {
    fn header_write(&self, h: &AOStorageParams) -> Result<()>;
    fn header_read(&self) -> Result<AOStorageParams>;

    fn size(&self) -> usize;

    fn write_u8(&self, v: u8) -> Result<()>;
    fn write_u16(&self, v: u16) -> Result<()>;
    fn write_u32(&self, v: u32) -> Result<()>;
    fn write_u64(&self, v: u64) -> Result<()>;

    fn read_u8(&self, seek: usize) -> Result<u8>;
    fn read_u16(&self, seek: usize) -> Result<u16>;
    fn read_u32(&self, seek: usize) -> Result<u32>;
    fn read_u64(&self, seek: usize) -> Result<u64>;
}
#[derive(Clone, Copy)]
pub struct AOStorageParams {
    tree_params: TreeParams,
}

impl AOStorageParams {
    pub fn default() -> Self {
        Self {
            tree_params: TreeParams::default(),
        }
    }
}

pub struct AOStorage<'a, Store: AppendOnlyStruct> {
    store: &'a Store,
    params: AOStorageParams,
    cmp: &'a HashMap<u32, PageKeyCmpRc>,
    trans: HashMap<u32, Transaction>,
}

impl<'a, Store: AppendOnlyStruct> AOStorage<'a, Store> {
    pub fn new(
        s: &'a Store,
        params: &AOStorageParams,
        cmp: &'a HashMap<u32, PageKeyCmpRc>,
    ) -> Result<Self> {
        s.header_write(&params)?;

        Ok(AOStorage {
            store: s,
            params: params.clone(),
            cmp: cmp,
            trans: HashMap::new(),
        })
    }

    pub fn open(s: &'a Store, cmp: &'a HashMap<u32, PageKeyCmpRc>) -> Result<Self> {
        let params = s.header_read()?;
        Ok(AOStorage {
            store: s,
            cmp: cmp,
            params: params,
            trans: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        todo!();
    }

    fn insert_kv(&self, key: &[u8], data: &[u8]) -> Result<u32> {
        // datalist::insert(self.space, data_offset, key, data);
        todo!()
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        // let tparams = self.params.tree_params.clone();
        // let data_size = datalist::get_pack_size(key, data);
        // let data_offset = self.get_mem(data_size, false, false)?;

        // let key_offset = self.insert_kv(key, data)?;
        // let mut trans = if let Some(t) = self.trans.get(&tree_id) {
        //     old_trans_offset = Some(t.offset());
        //     old_trans_size = t.size() as usize;
        //     Transaction::from_transaction(t)
        // } else {
        //     Transaction::new(0, tree_id, tparams.clone(), self.get_cmp(tree_id))
        // };
        // let root = if let Some(r) = trans.get_root() {
        //     r.clone()
        // } else {
        //     let res = Node::new_leaf_with_size(Id(1), tparams.t);
        //     trans.add_node(&res);
        //     res
        // };

        // let _insert_res = crate::tree::insert::insert(
        //     &mut trans,
        //     &root,
        //     key_offset,
        //     &crate::tree::record::Record::from_u32(key_offset),
        // )?;

        // self.save_trans(true, trans)?;
        // if old_trans_offset.is_some() {
        //     self.free_mem(old_trans_offset.unwrap(), old_trans_size)?;
        // }

        todo!();
    }

    pub fn find(&mut self, tree_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{page::PageKeyCmp, types::SingleElementStore, utils::any_as_u8_slice, Result};

    struct MockStorageKeyCmp {}

    impl MockStorageKeyCmp {
        fn new() -> MockStorageKeyCmp {
            MockStorageKeyCmp {}
        }
    }

    impl PageKeyCmp for MockStorageKeyCmp {
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
        let mut all_cmp = HashMap::new();
        let cmp: PageKeyCmpRc = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp.clone());

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
