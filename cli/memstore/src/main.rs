use std::{cell::RefCell, collections::HashMap, io::Write, rc::Rc, time::Instant};

use bpts::{
    page::{PageKeyCmp, PageKeyCmpRc},
    prelude::Result,
    storage::{FlatStorage, Storage, StorageParams},
    types::SingleElementStore,
    utils::any_as_u8_slice,
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

fn main() -> Result<()> {
    let count = 10000;
    let mut all_cmp = HashMap::new();
    let cmp: PageKeyCmpRc = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
    all_cmp.insert(1u32, cmp.clone());

    let fstore = MockPageStorage::new();
    Storage::init(&fstore, &StorageParams::default())?;

    let mut store = Storage::open(&fstore, &all_cmp)?;

    let full_time_begin = Instant::now();
    for key in 0..count {
        let cur_begin = Instant::now();
        // let info = store.info()?;
        // for rinfo in info {
        //     print!("{}", rinfo);
        // }
        // println!();
        let cur_key_sl = unsafe { any_as_u8_slice(&key) };
        store.insert(1, &cur_key_sl, &cur_key_sl)?;
        {
            let find_res = store.find(1, cur_key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, cur_key_sl)
        }
        let cur_duration = cur_begin.elapsed();
        let info = store.info()?;
        print!(
            "\rwrite  cur:{}% blocks:{} time:{:?}",
            (100f32 * key as f32) / (count as f32),
            info.len(),
            cur_duration
        );
        let _ = std::io::stdout().flush();
    }
    let duration = full_time_begin.elapsed();
    println!("\nwrite:{:?}", duration);
    Ok(())
}
