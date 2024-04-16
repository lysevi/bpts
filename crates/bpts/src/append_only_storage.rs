use std::collections::HashMap;
use std::fs::File;

use crate::page::PageKeyCmpRc;
use crate::tree::params::{self, TreeParams};
use crate::Result;

/*
params:.... MAGIC_NUMBERkey+data...tree...MAGIC_NUMBERtransaction_list
transaction_list - set links to trees.
 */

pub trait AppendOnlyStruct {
    fn header_write(&self, h: &AOStorageParams) -> Result<()>;
    fn header_read(&self) -> Result<AOStorageParams>;

    fn size(&self) -> usize;
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
        })
    }

    pub fn open(s: &'a Store, cmp: &'a HashMap<u32, PageKeyCmpRc>) -> Result<Self> {
        let params = s.header_read()?;
        Ok(AOStorage {
            store: s,
            cmp: cmp,
            params: params,
        })
    }

    pub fn close(&mut self) -> Result<()> {
        todo!();
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
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
    use crate::{page::PageKeyCmp, utils::any_as_u8_slice, Result};

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
        space: RefCell<Vec<u8>>,
    }

    impl MockPageStorage {
        pub fn new() -> MockPageStorage {
            MockPageStorage {
                space: RefCell::new(Vec::new()),
            }
        }
    }

    impl AppendOnlyStruct for MockPageStorage {
        fn header_write(&self, h: &AOStorageParams) -> Result<()> {
            todo!()
        }

        fn header_read(&self) -> Result<AOStorageParams> {
            todo!()
        }

        fn size(&self) -> usize {
            todo!()
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
