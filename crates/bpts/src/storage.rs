use crate::{freelist::FreeList, page::Page, tree::params::TreeParams, Result};

/*
DbHeader:DataBlock;Page1;Page2...PageN:DataBlock
*/

pub trait FlatStorage {
    fn header_write(&self, h: &StorageParams) -> Result<()>;
    fn header_read(&self) -> Result<Option<*const StorageParams>>;

    fn alloc_region(&self, size: u32) -> Result<()>;
}
#[derive(Clone, Copy)]
pub struct StorageParams {
    is_closed_normally: bool,
    cluster_size: u16,
    page_size: u32,
    freepagelist_size: u32,
}

impl StorageParams {
    pub fn default() -> StorageParams {
        StorageParams {
            is_closed_normally: true,
            cluster_size: 16,
            page_size: 1024,
            freepagelist_size: 16,
        }
    }
}

pub struct DataBlockHeader {
    pub freelist_size: u32,
    pub next_data_block_offset: u32,
}

pub struct Storage<'a, PS: FlatStorage> {
    pstore: &'a PS,
    params: Option<*const StorageParams>,
}

impl<'a, PS> Storage<'a, PS>
where
    PS: FlatStorage,
{
    pub fn new(pstore: &'a PS) -> Storage<PS> {
        Storage {
            pstore: pstore,
            params: None,
        }
    }

    pub fn init(&self, params: StorageParams) -> Result<()> {
        let hdr = StorageParams {
            is_closed_normally: false,
            ..params
        };

        self.pstore.header_write(&hdr)?;
        let tparam = TreeParams::default();
        let page_free_list = FreeList::calc_full_size(hdr.freepagelist_size, 1);
        let page_full_size = Page::calc_size(tparam, params.page_size, params.cluster_size);
        self.pstore.alloc_region(page_free_list + page_full_size)?;

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
    use std::cell::RefCell;

    use super::*;
    use crate::{types::SingleElementStore, Result};

    struct MockPageStorage {
        hdr: RefCell<SingleElementStore<StorageParams>>,
    }

    impl MockPageStorage {
        pub fn new() -> MockPageStorage {
            MockPageStorage {
                hdr: RefCell::new(SingleElementStore::new()),
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
            todo!();
            return Ok(());
        }
    }

    #[test]
    fn db() -> Result<()> {
        let fstore = MockPageStorage::new();
        let mut store = Storage::new(&fstore);
        store.init(StorageParams::default())?;

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
