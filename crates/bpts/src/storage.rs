use std::{collections::HashMap, mem::offset_of, ptr};

use crate::{
    freelist::FreeList,
    page::{self, Page, PageKeyCmpRc},
    tree::params::TreeParams,
    Result,
};

/*
DbHeader:DataBlock;PagesFreeList;Page1;Page2...PageN:DataBlock
*/

pub trait FlatStorage {
    fn header_write(&self, h: &StorageParams) -> Result<()>;
    fn header_read(&self) -> Result<Option<*const StorageParams>>;

    fn alloc_region(&self, size: u32) -> Result<()>;
    fn space_ptr(&self) -> Result<*mut u8>;
}
#[derive(Clone, Copy)]
pub struct StorageParams {
    is_closed_normally: bool,
    cluster_size: u16,
    page_size: u32,
    freepagelist_len: u32,
    tree_params: TreeParams,
}

impl StorageParams {
    pub fn default() -> StorageParams {
        StorageParams {
            is_closed_normally: true,
            cluster_size: 16,
            page_size: 1024,
            freepagelist_len: 16,
            tree_params: TreeParams::default(),
        }
    }
}

pub struct DataBlockHeader {
    pub freelist_size: u32,
    pub page_full_size: u32,
    pub next_data_block_offset: u32,
}

const DATABLOCKHEADERSIZE: u32 = std::mem::size_of::<DataBlockHeader>() as u32;

pub struct Storage<'a, PS: FlatStorage> {
    pstore: &'a PS,
    params: Option<*const StorageParams>,
    freelist: Option<FreeList>,
    space: *mut u8,
    cmp: Option<&'a HashMap<u32, PageKeyCmpRc>>,
}

impl<'a, PS> Storage<'a, PS>
where
    PS: FlatStorage,
{
    pub fn new(pstore: &'a PS) -> Storage<PS> {
        Storage {
            pstore: pstore,
            params: None,
            freelist: None,
            cmp: None,
            space: ptr::null_mut(),
        }
    }

    pub fn init(pstore: &'a PS, params: &StorageParams) -> Result<()> {
        let hdr = StorageParams {
            is_closed_normally: true,
            ..*params
        };

        pstore.header_write(&hdr)?;
        let tparam = params.tree_params.clone();
        let page_free_list_size = FreeList::calc_full_size(hdr.freepagelist_len, 1);
        let page_full_size = Page::calc_size(tparam, params.page_size, params.cluster_size);

        let dblock = DataBlockHeader {
            freelist_size: page_free_list_size,
            page_full_size: page_full_size,
            next_data_block_offset: 0,
        };

        pstore.alloc_region(DATABLOCKHEADERSIZE + page_free_list_size + page_full_size)?;
        let space = pstore.space_ptr()?;
        unsafe {
            (space as *mut DataBlockHeader).write(dblock);
            FreeList::init(
                space.add(DATABLOCKHEADERSIZE as usize),
                hdr.freepagelist_len,
                1,
            );
        }

        Ok(())
    }

    pub fn open(pstore: &'a PS, cmp: &'a HashMap<u32, PageKeyCmpRc>) -> Result<Storage<'a, PS>> {
        let mut result = Storage::new(pstore);
        result.cmp = Some(cmp);
        let p = pstore.header_read()?;
        result.params = p;
        unsafe {
            if (*result.params.unwrap()).is_closed_normally == false {
                todo!()
            }
        }
        //let sparams = result.get_storage_params().unwrap();
        let space = pstore.space_ptr()?;

        let freelist_space = unsafe { space.add(DATABLOCKHEADERSIZE as usize) };

        let freelist = unsafe { FreeList::open(freelist_space) };
        result.space = space;

        result.freelist = Some(freelist);
        result.space = space;
        result.pstore = pstore;
        Ok(result)
    }

    pub fn close(&self) -> Result<()> {
        unsafe {
            let mut params = *(self.params.unwrap());
            params.is_closed_normally = true;
            self.pstore.header_write(&params)?;
        }
        Ok(())
    }
    pub fn get_storage_params(&self) -> Option<StorageParams> {
        return match self.params {
            Some(x) => Some(unsafe { (*x).clone() }),
            None => None,
        };
    }

    pub fn insert(&mut self, tree_id: u32, key: &[u8], data: &[u8]) -> Result<()> {
        let mut target_page: Option<Page> = None;
        match self.freelist {
            Some(ref mut fl) => unsafe {
                for i in 0..fl.len() {
                    let page_state = fl.get(i)?;
                    if page_state == 1 {
                        let dblock = (self.space as *mut DataBlockHeader).read();
                        let offset = DATABLOCKHEADERSIZE
                            + dblock.freelist_size
                            + (dblock.page_full_size * i as u32);
                        let page = Page::from_buf(
                            self.space.add(offset as usize),
                            self.cmp.unwrap().clone(),
                        )?;
                        target_page = Some(page);
                        break;
                    }
                    if page_state == 0 {
                        fl.set(i, 1)?;
                        let params = (*self.params.unwrap()).clone();
                        let dblock = (self.space as *mut DataBlockHeader).read();
                        let offset = DATABLOCKHEADERSIZE
                            + dblock.freelist_size
                            + (dblock.page_full_size * i as u32);
                        let page = Page::init_buffer(
                            self.space.add(offset as usize),
                            params.page_size,
                            params.cluster_size,
                            self.cmp.unwrap().clone(),
                            params.tree_params,
                        )?;
                        target_page = Some(page);
                        break;
                    }
                }
            },
            None => panic!(),
        }

        match target_page {
            Some(ref mut x) => {
                let res = x.insert(tree_id, key, data);
                return res;
            }
            None => panic!(),
        }
    }

    pub fn find(&self, tree_id: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut target_page: Option<Page> = None;
        match self.freelist {
            Some(ref fl) => unsafe {
                for i in 0..fl.len() {
                    let page_state = fl.get(i)?;
                    if page_state == 1 {
                        let params = (*self.params.unwrap()).clone();
                        let dblock = (self.space as *mut DataBlockHeader).read();
                        let offset = DATABLOCKHEADERSIZE
                            + dblock.freelist_size
                            + (params.page_size + dblock.freelist_size) * i as u32;
                        let page = Page::from_buf(
                            self.space.add(offset as usize),
                            self.cmp.unwrap().clone(),
                        )?;
                        target_page = Some(page);
                        break;
                    }
                }
            },
            None => panic!(),
        }

        match target_page {
            Some(ref mut x) => {
                let res = x.find(tree_id, key)?;
                return match res {
                    Some(d) => {
                        let result = d.to_vec();
                        Ok(Some(result))
                    }
                    None => Ok(None),
                };
            }
            None => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{
        page::PageKeyCmp, types::SingleElementStore, utils::any_as_u8_slice, verbose, Result,
    };

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
    }

    impl FlatStorage for MockPageStorage {
        fn header_write(&self, h: &StorageParams) -> Result<()> {
            self.hdr.borrow_mut().replace(h.clone());
            Ok(())
        }

        fn header_read(&self) -> Result<Option<*const StorageParams>> {
            if !self.hdr.borrow().is_empty() {
                let rf = self.hdr.borrow_mut();
                let ptr = rf.as_ptr();
                return Ok(Some(ptr));
            }
            return Ok(None);
        }

        fn alloc_region(&self, size: u32) -> Result<()> {
            let old_len = self.space.borrow().len();
            self.space.borrow_mut().resize(old_len + size as usize, 0u8);
            return Ok(());
        }

        fn space_ptr(&self) -> Result<*mut u8> {
            Ok(self.space.borrow_mut().as_mut_ptr())
        }
    }

    #[test]
    fn db() -> Result<()> {
        let mut all_cmp = HashMap::new();
        let cmp: PageKeyCmpRc = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp.clone());

        let fstore = MockPageStorage::new();
        Storage::init(&fstore, &StorageParams::default())?;

        let writed_header = fstore.header_read()?;
        assert!(writed_header.is_some());
        let writed_header = writed_header.unwrap();
        unsafe {
            assert_eq!((*writed_header).cluster_size, 16);
            assert_eq!((*writed_header).page_size, 1024);
        }

        let mut store = Storage::open(&fstore, &all_cmp)?;
        let writed_params = store.get_storage_params();
        assert!(writed_params.is_some());
        assert_eq!(writed_params.unwrap().cluster_size, 16);
        assert_eq!(writed_params.unwrap().page_size, 1024);

        for key in 0..3 {
            println!("insert {}", key);
            let key_sl = unsafe { any_as_u8_slice(&key) };
            store.insert(1, &key_sl, &key_sl)?;
            let find_res = store.find(1, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        store.close()?;
        Ok(())
    }
}
