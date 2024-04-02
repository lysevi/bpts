use std::{collections::HashMap, ptr};

use crate::{
    freelist::FreeList,
    page::{Page, PageKeyCmpRc},
    tree::params::TreeParams,
    Result,
};

/*
DbHeader:DataBlock;Page1;Page2...PageN:DataBlock
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
    treeParams: TreeParams,
}

impl StorageParams {
    pub fn default() -> StorageParams {
        StorageParams {
            is_closed_normally: true,
            cluster_size: 16,
            page_size: 1024,
            freepagelist_len: 16,
            treeParams: TreeParams::default(),
        }
    }
}

pub struct DataBlockHeader {
    pub freelist_size: u32,
    pub next_data_block_offset: u32,
}

const DATABLOCKHEADERSIZE: u32 = std::mem::size_of::<DataBlockHeader>() as u32;

pub struct Storage<'a, PS: FlatStorage> {
    pstore: &'a PS,
    params: Option<*const StorageParams>,
    freelist: Option<FreeList>,
    curpage: Option<Page>,
    space: *mut u8,
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
            curpage: None,
            space: ptr::null_mut(),
        }
    }

    pub fn init(&mut self, params: StorageParams, cmp: HashMap<u32, PageKeyCmpRc>) -> Result<()> {
        let hdr = StorageParams {
            is_closed_normally: false,
            ..params
        };

        self.pstore.header_write(&hdr)?;
        let tparam = TreeParams::default();
        let page_free_list = FreeList::calc_full_size(hdr.freepagelist_len, 1);
        let page_full_size = Page::calc_size(tparam, params.page_size, params.cluster_size);

        let dblock = DataBlockHeader {
            freelist_size: page_free_list,
            next_data_block_offset: 0,
        };

        self.pstore
            .alloc_region(DATABLOCKHEADERSIZE + page_free_list + page_full_size)?;

        let mut space = self.pstore.space_ptr()?;
        unsafe { (space as *mut DataBlockHeader).write(dblock) };

        let freelist_space = unsafe { space.add(DATABLOCKHEADERSIZE as usize) };

        let mut freelist = unsafe { FreeList::init(freelist_space, hdr.freepagelist_len, 1) };
        self.space = unsafe { space.add(page_free_list as usize) };
        unsafe {
            freelist.set(0, true)?;
            let page = Page::init_buffer(
                self.space,
                hdr.page_size,
                hdr.cluster_size,
                cmp,
                hdr.treeParams,
            )?;
            self.curpage = Some(page)
        };
        self.freelist = Some(freelist);

        Ok(())
    }

    pub fn open(&mut self) -> Result<()> {
        let p = self.pstore.header_read()?;
        self.params = p;
        Ok(())
    }

    pub fn get_params(&self) -> Option<StorageParams> {
        return match self.params {
            Some(x) => Some(unsafe { (*x).clone() }),
            None => None,
        };
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{page::PageKeyCmp, types::SingleElementStore, Result};

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
        let mut allCmp = HashMap::new();
        let cmp: PageKeyCmpRc = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        allCmp.insert(0u32, cmp.clone());

        let fstore = MockPageStorage::new();
        let mut store = Storage::new(&fstore);
        store.init(StorageParams::default(), allCmp)?;

        let writed_header = fstore.header_read()?;
        assert!(writed_header.is_some());
        let writed_header = writed_header.unwrap();
        unsafe {
            assert_eq!((*writed_header).cluster_size, 16);
            assert_eq!((*writed_header).page_size, 1024);
        }
        store.open()?;
        let writed_params = store.get_params();
        assert!(writed_params.is_some());
        assert_eq!(writed_params.unwrap().cluster_size, 16);
        assert_eq!(writed_params.unwrap().page_size, 1024);
        Ok(())
    }
}
