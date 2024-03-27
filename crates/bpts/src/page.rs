use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::freelist::FreeList;
use crate::transaction::{TransKeyCmp, Transaction};
use crate::tree::node::{KeyCmp, Node};
use crate::tree::nodestorage::NodeStorage;
use crate::tree::params::TreeParams;
use crate::types::Id;
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
    user_key: Option<Vec<u8>>,
    buffer: *const u8,
    cmp: PageKeyCmpRc,
}

impl PageKeyCmpRef {
    fn cmp_with_left(&self, key2: u32) -> std::cmp::Ordering {
        let other = unsafe { datalist::load_key(self.buffer, key2) };
        if let Some(x) = &self.user_key {
            return self.cmp.borrow().compare(x.as_slice(), other);
        }
        panic!();
    }

    fn cmp_with_right(&self, key1: u32) -> std::cmp::Ordering {
        let other = unsafe { datalist::load_key(self.buffer, key1) };
        if let Some(x) = &self.user_key {
            return self.cmp.borrow().compare(other, x.as_slice());
        }
        panic!();
    }
}

impl KeyCmp for PageKeyCmpRef {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        if key1 == std::u32::MAX && key2 == std::u32::MAX {
            return std::cmp::Ordering::Equal;
        }
        if key1 == std::u32::MAX {
            return self.cmp_with_left(key2);
        }

        if key1 != std::u32::MAX && key2 != std::u32::MAX {
            let k1 = unsafe { datalist::load_key(self.buffer, key1) };
            let k2 = unsafe { datalist::load_key(self.buffer, key2) };
            return self.cmp.borrow().compare(k1, k2);
        }

        return self.cmp_with_right(key1);
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
            user_key: None,
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

    pub fn clusters_for_bytes(&self, size: usize) -> usize {
        let size_in_clusters = unsafe { (size as f32) / ((*self.hdr).cluster_size as f32) };
        let clusters_need = (size_in_clusters).ceil() as usize;
        return clusters_need;
    }

    unsafe fn offset_of_cluster(&self, cluster: usize) -> u32 {
        cluster as u32 * (*self.hdr).cluster_size as u32
    }

    pub fn save_trans(&mut self, t: Transaction) -> Result<()> {
        //TODO! status enum
        let neeed_bytes = t.size();
        let old_translist_size = std::mem::size_of::<u32>() * (self.trans.len() + 1);
        unsafe {
            {
                let offset = self.get_mem(neeed_bytes as usize, true)?;

                let mut target = t;

                let ptr = self.space.add(offset as usize);
                let writed_bytes = target.save_to(ptr, offset);
                assert_eq!(writed_bytes, neeed_bytes);

                let tree_id = target.tree_id();
                self.trans.insert(tree_id, target);
            }
            {
                let neeed_bytes = (std::mem::size_of::<u32>() * (self.trans.len() + 1)) as u32;

                let trans_list_offset = self.get_mem(neeed_bytes as usize, true)?;

                let mut ptr = self.space.add(trans_list_offset as usize);

                let count_ptr = ptr as *mut u32;
                let count = self.trans.len() as u32;
                std::ptr::copy(&count, count_ptr, 1);
                for trans in self.trans.iter() {
                    ptr = ptr.add(std::mem::size_of::<u32>());
                    let offset_ptr = ptr as *mut u32;
                    let value = trans.1.offset();
                    std::ptr::copy(&value, offset_ptr, 1);
                }

                let old_trans_list = (*self.hdr).trans_list_offset;
                (*self.hdr).trans_list_offset = trans_list_offset;
                if old_trans_list != 0 {
                    self.free_mem(old_trans_list, old_translist_size)?
                }
            }
        }

        Ok(())
    }

    pub fn free_clusters_count(&self) -> usize {
        unsafe { self.freelist.free_clusters() }
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
        unsafe { self.freelist.is_full() }
    }

    pub fn tree_params(&self) -> TreeParams {
        return unsafe { (*self.hdr).params };
    }

    pub fn get_cmp(&self) -> TransKeyCmp {
        let result = PageKeyCmpRef {
            user_key: None,
            cmp: self.cmp.clone(),
            buffer: self.space,
        };
        return Rc::new(RefCell::new(result));
    }

    //TODO! enum for write status
    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        let tparams = self.tree_params();

        let data_size = datalist::get_pack_size(key, data);
        let data_offset = self.get_mem(data_size, false)?;

        let key_offset = unsafe { datalist::insert(self.space, data_offset, key, data) };

        let mut old_trans_offset = None;
        let mut old_trans_size = 0usize;
        let mut trans = if let Some(t) = self.trans.get(&tree_id) {
            old_trans_offset = Some(t.offset());
            old_trans_size = t.size() as usize;
            Transaction::from_transaction(t)
        } else {
            Transaction::new(0, tree_id, tparams.clone(), self.get_cmp())
        };

        let root = if let Some(r) = trans.get_root() {
            r.clone()
        } else {
            let res = Node::new_leaf_with_size(Id(1), tparams.t);
            trans.add_node(&res);
            res
        };

        let _insert_res = crate::tree::insert::insert(
            &mut trans,
            &root,
            key_offset,
            &crate::tree::record::Record::from_u32(key_offset),
        )?;

        self.save_trans(trans)?;
        if old_trans_offset.is_some() {
            self.free_mem(old_trans_offset.unwrap(), old_trans_size)?;
        }
        Ok(())
    }

    pub fn find<'a>(&self, tree_id: u32, key: &'a [u8]) -> Result<Option<&'a [u8]>> {
        if let Some(t) = self.trans.get(&tree_id) {
            let mut trans = Transaction::from_transaction(t);
            if let Some(root) = trans.get_root() {
                let etalon_key = key.to_vec();
                let cmp = Rc::new(RefCell::new(PageKeyCmpRef {
                    user_key: Some(etalon_key),
                    cmp: self.cmp.clone(),
                    buffer: self.space,
                }));
                trans.set_cmp(cmp);

                let find_res = crate::tree::read::find(&mut trans, &root.clone(), std::u32::MAX)?;
                if find_res.is_none() {
                    return Ok(None);
                }
                let data = unsafe { datalist::load_data(self.space, find_res.unwrap().into_u32()) };
                return Ok(Some(data));
            }
        }
        return Ok(None);
    }

    //TODO enum for remove status
    pub fn remove(&mut self, tree_id: u32, key: &[u8]) -> Result<()> {
        if let Some(t) = self.trans.get(&tree_id) {
            let old_trans_offset = t.offset();
            let old_trans_size = t.size() as usize;
            let mut trans = Transaction::from_transaction(t);
            if let Some(root) = trans.get_root() {
                let etalon_key = key.to_vec();
                let cmp = Rc::new(RefCell::new(PageKeyCmpRef {
                    user_key: Some(etalon_key),
                    cmp: self.cmp.clone(),
                    buffer: self.space,
                }));
                trans.set_cmp(cmp);

                let res = crate::tree::remove::remove_key_with_data(
                    &mut trans,
                    &root.clone(),
                    std::u32::MAX,
                )?;
                self.save_trans(trans)?;
                self.free_mem(old_trans_offset, old_trans_size)?;

                let removed_data = res.0;

                let data_size = unsafe { datalist::load_size(self.space, removed_data.into_u32()) };

                self.free_mem(removed_data.into_u32(), data_size as usize)?;
                return Ok(());
            }
        }
        return Ok(());
    }

    //TODO enum for from_top
    fn get_mem(&mut self, data_size: usize, from_top: bool) -> Result<u32> {
        let clusters_need = self.clusters_for_bytes(data_size);
        let data_cluster = if from_top {
            unsafe { self.freelist.get_region_top(clusters_need) }
        } else {
            unsafe { self.freelist.get_region_bottom(clusters_need) }
        };
        if data_cluster.is_none() {
            return Err(crate::Error("no space left".to_owned()));
        }
        let first_cluster = data_cluster.unwrap();
        for i in 0..clusters_need {
            unsafe { self.freelist.set(first_cluster + i, true)? };
        }

        let data_offset = unsafe { self.offset_of_cluster(data_cluster.unwrap()) };
        return Ok(data_offset);
    }

    fn free_mem(&mut self, old_trans_offset: u32, old_trans_size: usize) -> Result<()> {
        let cluster_num =
            unsafe { old_trans_offset as f32 / ((*self.hdr).cluster_size as f32) } as usize;
        let clusteres_count = self.clusters_for_bytes(old_trans_size);

        for i in 0..clusteres_count {
            unsafe { self.freelist.set(cluster_num + i, false)? };
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crate::tree::params::TreeParams;

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
    fn insert_find_in_full() -> Result<()> {
        let tparam = TreeParams::default();
        let pagedatasize = 1024 * 20;
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
        let mut all_keys = Vec::new();

        while !page.is_full() {
            let key_sl = unsafe { any_as_u8_slice(&key) };
            println!("insert: {}", key);
            // if key == 200 {
            //     println!("!");
            // }
            let write_res = page.insert(0u32, key_sl, key_sl);

            if write_res.is_err() {
                break;
            }

            all_keys.push(key);
            key += 1;

            for item in all_keys.iter() {
                //println!("find: {}", item);
                let key_sl = unsafe { any_as_u8_slice(item) };
                let result = page.find(0u32, key_sl)?;
                assert!(result.is_some());
                assert_eq!(key_sl, result.unwrap());
            }
        }

        assert!(all_keys.len() > 1);
        println!("keys: {}", all_keys.len());
        // while all_keys.len() > 0 {
        //     {
        //         let item = all_keys.first().unwrap();
        //         println!("remove: {}", item);
        //         let key_sl = unsafe { any_as_u8_slice(item) };
        //         page.remove(0u32, key_sl)?;
        //         let result = page.find(0u32, key_sl)?;
        //         assert!(result.is_none());
        //     }
        //     all_keys.remove(0);
        //     for i in all_keys.iter() {
        //         let key_sl = unsafe { any_as_u8_slice(i) };
        //         let result = page.find(0u32, key_sl)?;
        //         assert!(result.is_some());
        //     }
        // }

        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }
        Ok(())
    }

    #[test]
    fn insert_find_delete() -> Result<()> {
        let tparam = TreeParams::default();
        let pagedatasize = 1024 * 1024 * 1024;
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
        let mut all_keys = Vec::new();

        for _i in 0..(page.tree_params().t * 2) {
            let key_sl = unsafe { any_as_u8_slice(&key) };
            println!("insert: {}", key);
            // if key == 200 {
            //     println!("!");
            // }
            let write_res = page.insert(0u32, key_sl, key_sl);

            if write_res.is_err() {
                break;
            }

            all_keys.push(key);
            key += 1;

            for item in all_keys.iter() {
                //println!("find: {}", item);
                let key_sl = unsafe { any_as_u8_slice(item) };
                let result = page.find(0u32, key_sl)?;
                assert!(result.is_some());
                assert_eq!(key_sl, result.unwrap());
            }
        }

        assert!(all_keys.len() > 1);
        let free_clusters = page.free_clusters_count();

        while all_keys.len() > 0 {
            {
                let item = all_keys.first().unwrap();
                println!("remove: {}", item);
                // if *item == 102 {
                //     println!("!");
                // }
                let key_sl = unsafe { any_as_u8_slice(item) };
                page.remove(0u32, key_sl)?;
                let result = page.find(0u32, key_sl)?;
                assert!(result.is_none());
            }
            all_keys.remove(0);
            for i in all_keys.iter() {
                let key_sl = unsafe { any_as_u8_slice(i) };
                let result = page.find(0u32, key_sl)?;
                assert!(result.is_some());
                assert_eq!(key_sl, result.unwrap());
            }
        }

        assert!(free_clusters < page.free_clusters_count());

        for i in 0..10 {
            let pos = b.len() - 1 - i;
            b[pos] = i as u8;
        }
        Ok(())
    }
}
