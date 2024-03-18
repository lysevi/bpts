use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use bpts_tree::node::KeyCmp;
use bpts_tree::params::TreeParams;

use crate::freelist::FreeList;
use crate::transaction::{TransKeyCmp, Transaction};
use crate::{datalist, freelist, prelude::*};

pub trait PageKeyCmp {
    fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering;
}

pub type PageKeyCmpRc = Rc<RefCell<dyn PageKeyCmp>>;

/*Page:
0:[HEAD]
[freelist]
space:{
    Trans1,Trans1.1,TransList,Trans2.1,TransList
    ....
    Data4,Data3,Data2,Data1
}
TransList: count_u32 [offset of transactions]
*/
#[repr(C, packed)]
struct Header {
    pub id: u32,
    pub trans_list_offset: u32,
    pub cluster_size: u16,
    pub freelist_size: u32,
    params: TreeParams,
}

const HEADER_SIZE: usize = std::mem::size_of::<Header>();

impl Header {
    fn default(params: TreeParams, buffsize: u32, cluster_size: u16) -> Header {
        Header {
            id: 0,
            trans_list_offset: 0,
            cluster_size,
            freelist_size: freelist::FreeList::calc_size(buffsize, cluster_size),
            params,
        }
    }
}

struct PageKeyCmpRef {
    buffer: *const u8,
    cmp: PageKeyCmpRc,
}

impl KeyCmp for PageKeyCmpRef {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        let k1 = unsafe { datalist::load_key(self.buffer, key1) };
        let k2 = unsafe { datalist::load_key(self.buffer, key2) };
        self.cmp.borrow().compare(k1.1, k2.1)
    }
}

pub struct Page {
    space: *mut u8,
    freelist: FreeList,
    hdr: *mut Header,
    trans: HashMap<u32, Transaction>,
    cmp: PageKeyCmpRc,
}

impl Page {
    pub unsafe fn init_buffer(
        buffer: *mut u8,
        buffsize: u32,
        cluster_size: u16,
        cmp: PageKeyCmpRc,
        params: TreeParams,
    ) -> Result<Page> {
        let result: Page;

        let h = buffer as *mut Header;

        (*h) = Header::default(params, buffsize, cluster_size);
        (*h).params = params;

        let flsize = (*h).freelist_size;
        let mut fl = FreeList::new(buffer.add(HEADER_SIZE), flsize);
        fl.init();
        let space = buffer
            .add(HEADER_SIZE)
            .add(freelist::FreeList::size_for_len(flsize) as usize);
        let t = HashMap::new();

        result = Page {
            hdr: h,
            freelist: fl,
            trans: t,
            space: space,
            cmp: cmp,
        };

        return Ok(result);
    }

    pub unsafe fn from_buf(buffer: *mut u8, cmp: PageKeyCmpRc) -> Result<Page> {
        let result: Page;

        let h = buffer as *mut Header;
        let flsize = (*h).freelist_size;
        let space = buffer
            .add(HEADER_SIZE)
            .add(freelist::FreeList::size_for_len(flsize) as usize);
        let transcmp = Rc::new(RefCell::new(PageKeyCmpRef {
            cmp: cmp.clone(),
            buffer: space,
        }));

        let t: HashMap<u32, Transaction> = if (*h).trans_list_offset == 0 {
            HashMap::new()
        } else {
            let mut ptr = space.add((*h).trans_list_offset as usize);
            let count = (ptr as *const u32).read();
            let mut transes = HashMap::new();

            for _i in 0..count {
                ptr = ptr.add(std::mem::size_of::<u32>());
                let offset = (ptr as *const u32).read();
                let cur_trans_offset = space.add(offset as usize);
                let cur_trans = Transaction::from_buffer(
                    cur_trans_offset,
                    offset,
                    transcmp.clone(),
                    (*h).params,
                );
                transes.insert(cur_trans.tree_id(), cur_trans);
            }

            transes
        };
        result = Page {
            hdr: h,
            trans: t,
            space: space,
            cmp: cmp,
            freelist: FreeList::new(buffer.add(HEADER_SIZE), flsize),
        };

        return Ok(result);
    }

    pub fn calc_size(params: TreeParams, buffsize: u32, cluster_size: u16) -> u32 {
        let defparam = Header::default(params, buffsize, cluster_size);
        let freelistsize = freelist::FreeList::calc_size(buffsize, defparam.cluster_size);
        let result = HEADER_SIZE as u32 + buffsize + freelistsize;
        return result;
    }

    pub fn save_trans(&mut self, t: Transaction) -> Result<()> {
        //TODO! status enum
        let neeed_bytes = t.size();
        unsafe {
            let size_in_clusters = (neeed_bytes as f32) / ((*self.hdr).cluster_size as f32);
            let clusters_need = (size_in_clusters).ceil() as usize;

            let first_cluster = self.freelist.get_region_top(clusters_need);
            if first_cluster.is_none() {
                bpts_tree::types::Error("no space left".to_owned());
            }

            let first_cluster = first_cluster.unwrap();
            for i in 0..clusters_need {
                self.freelist.set(first_cluster + i, true)?;
            }

            let mut offset = first_cluster as u32 * (*self.hdr).cluster_size as u32;

            let mut target = t;
            let ptr = self.space.add(offset as usize);
            let writed_bytes = target.save_to(ptr, offset);
            assert_eq!(writed_bytes, neeed_bytes);
            offset += writed_bytes;

            self.trans.insert(target.tree_id(), target);

            let trans_list_offset = offset;

            let mut ptr = self.space.add(offset as usize);

            let count_ptr = ptr as *mut u32;
            let count = self.trans.len() as u32;
            std::ptr::copy(&count, count_ptr, 1);
            for trans in self.trans.iter() {
                ptr = ptr.add(std::mem::size_of::<u32>());
                let offset_ptr = ptr as *mut u32;
                let value = trans.1.offset();
                std::ptr::copy(&value, offset_ptr, 1);
            }

            (*self.hdr).trans_list_offset = trans_list_offset;
        }

        Ok(())
    }

    pub fn get_id(&self) -> u32 {
        let result = unsafe { (*self.hdr).id };
        return result;
    }

    pub fn set_id(&mut self, i: u32) {
        unsafe {
            (*self.hdr).id = i;
        }
    }

    pub fn transaction(&self, tree_id: u32) -> Option<Transaction> {
        if let Some(v) = self.trans.get(&tree_id) {
            return Some(v.clone());
        }
        None
    }

    pub fn trees_count(&self) -> usize {
        self.trans.len()
    }

    pub fn is_full(&self) -> bool {
        todo!()
    }

    pub fn tree_params(&self) -> TreeParams {
        return unsafe { (*self.hdr).params };
    }

    pub fn get_cmp(&self) -> TransKeyCmp {
        let result = PageKeyCmpRef {
            cmp: self.cmp.clone(),
            buffer: self.space,
        };
        return Rc::new(RefCell::new(result));
    }

    pub fn insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        todo!();
        Ok(())
    }

    pub fn find<'a>(&self, key: &[u8]) -> Result<Option<&'a [u8]>> {
        todo!();
        Ok(None)
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<()> {
        todo!();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    use bpts_tree::params::TreeParams;

    use super::Page;
    use crate::prelude::Result;
    use crate::transaction::Transaction;
    use crate::utils::any_as_u8_slice;

    use super::PageKeyCmp;

    struct MockKeyCmp {}

    impl MockKeyCmp {
        fn new() -> MockKeyCmp {
            MockKeyCmp {}
        }
    }

    impl PageKeyCmp for MockKeyCmp {
        fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
            key1.cmp(key2)
        }
    }

    #[test]
    fn page_from_buffer() -> Result<()> {
        let tparam = TreeParams::default();
        let pagedatasize = 1024;
        let cluster_size = 32;
        let bufsize = Page::calc_size(tparam, pagedatasize, cluster_size);
        let mut b = vec![0u8; bufsize as usize + 10];
        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }
        let cmp = Rc::new(RefCell::new(MockKeyCmp::new()));

        unsafe {
            {
                let mut page = Page::init_buffer(
                    b.as_mut_ptr(),
                    pagedatasize,
                    cluster_size,
                    cmp.clone(),
                    tparam.clone(),
                )?;
                assert_eq!(page.get_id(), 0);
                page.set_id(777);
            }
            {
                let deafult_params = TreeParams::default();
                let page2 = Page::from_buf(b.as_mut_ptr(), cmp.clone())?;
                assert_eq!(page2.get_id(), 777);
                let page_param = page2.tree_params();
                assert_eq!(page_param.t, deafult_params.t);
            }
            {
                let mut page = Page::from_buf(b.as_mut_ptr(), cmp.clone())?;
                let t = Transaction::new(3, 7, page.tree_params(), page.get_cmp());
                assert!(page.transaction(7).is_none());
                page.save_trans(t)?;
                assert_eq!(page.transaction(7).unwrap().rev(), 3);
            }
            {
                let mut page = Page::from_buf(b.as_mut_ptr(), cmp.clone())?;
                assert_eq!(page.transaction(7).unwrap().rev(), 3);
                assert_eq!(page.transaction(7).unwrap().tree_id(), 7);
                assert_eq!(page.trees_count(), 1);

                let t = Transaction::new(1, 8, page.tree_params(), page.get_cmp());
                assert!(!t.is_readonly());
                page.save_trans(t)?;
                assert_eq!(page.transaction(8).unwrap().rev(), 1);
                assert!(page.transaction(8).unwrap().is_readonly());
                assert_eq!(page.transaction(8).unwrap().tree_id(), 8);
                assert_eq!(page.trees_count(), 2);
            }

            {
                let mut page = Page::from_buf(b.as_mut_ptr(), cmp.clone())?;
                assert_eq!(page.trees_count(), 2);
                assert_eq!(page.transaction(7).unwrap().rev(), 3);
                assert_eq!(page.transaction(7).unwrap().tree_id(), 7);

                assert_eq!(page.transaction(8).unwrap().rev(), 1);
                assert_eq!(page.transaction(8).unwrap().tree_id(), 8);

                let t = Transaction::new(2, 8, page.tree_params(), page.get_cmp());
                page.save_trans(t)?;
            }

            {
                let page = Page::from_buf(b.as_mut_ptr(), cmp.clone())?;
                assert_eq!(page.trees_count(), 2);
                assert_eq!(page.transaction(7).unwrap().rev(), 3);
                assert_eq!(page.transaction(7).unwrap().tree_id(), 7);

                assert_eq!(page.transaction(8).unwrap().rev(), 2);
                assert_eq!(page.transaction(8).unwrap().tree_id(), 8);
            }
        }
        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }
        return Ok(());
    }

    #[test]
    fn insert_find_delete() -> Result<()> {
        let tparam = TreeParams::default();
        let pagedatasize = 1024 * 1024;
        let cluster_size = 32;
        let bufsize = Page::calc_size(tparam, pagedatasize, cluster_size);
        let cmp = Rc::new(RefCell::new(MockKeyCmp::new()));

        let mut b = vec![0u8; bufsize as usize + 10];

        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }

        let mut page = unsafe {
            Page::init_buffer(
                b.as_mut_ptr(),
                pagedatasize,
                cluster_size,
                cmp.clone(),
                tparam.clone(),
            )?
        };

        let mut key = 1;
        let mut all_keys = HashMap::new();

        while !page.is_full() {
            {
                let tmp = key;
                let key_sl = unsafe { any_as_u8_slice(&tmp) };
                page.insert(key_sl, key_sl)?;
            }
            all_keys.insert(key, key);
            key += 1;

            for item in all_keys.iter() {
                let key_sl = unsafe { any_as_u8_slice(&item.0) };
                let result = page.find(key_sl)?;
                assert!(result.is_some());
                assert_eq!(key_sl, result.unwrap());
            }
        }

        assert!(all_keys.len() > 1);

        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }
        Ok(())
    }
}
