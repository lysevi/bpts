use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// verbose output
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// data count
    #[arg(short, long, default_value_t = 10000)]
    count: i32,
}

use std::{cell::RefCell, collections::HashMap, io::Write, rc::Rc, time::Instant};

use bpts::{
    page::{PageKeyCmp, PageKeyCmpRc},
    prelude::Result,
    storage::{FlatStorage, Storage, StorageParams},
    types::SingleElementStore,
    utils::any_as_u8_slice,
};

#[derive(Clone)]
struct TestStorageInfo {
    allocations: usize,
    stat_miss_insert: usize,
    stat_miss_find: usize,
}

impl TestStorageInfo {
    pub fn new() -> TestStorageInfo {
        TestStorageInfo {
            allocations: 0,
            stat_miss_find: 0,
            stat_miss_insert: 0,
        }
    }
}

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
    info: RefCell<SingleElementStore<TestStorageInfo>>,
    space: RefCell<Vec<u8>>,
}

impl MockPageStorage {
    pub fn new() -> MockPageStorage {
        MockPageStorage {
            hdr: RefCell::new(SingleElementStore::new()),
            info: RefCell::new(SingleElementStore::new_with(TestStorageInfo::new())),
            space: RefCell::new(Vec::new()),
        }
    }

    pub fn get_info(&self) -> TestStorageInfo {
        self.info.borrow().as_value()
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
        let mut f = |info: &mut TestStorageInfo| info.allocations += 1;
        self.info.borrow_mut().apply(&mut f);

        let old_len = self.space.borrow().len();
        self.space.borrow_mut().resize(old_len + size as usize, 0u8);
        return Ok(());
    }

    fn stat_miss_find(&self) {
        let mut f = |info: &mut TestStorageInfo| info.stat_miss_find += 1;
        self.info.borrow_mut().apply(&mut f);
    }
    fn stat_miss_insert(&self) {
        let mut f = |info: &mut TestStorageInfo| info.stat_miss_insert += 1;
        self.info.borrow_mut().apply(&mut f);
    }

    fn space_ptr(&self) -> Result<*mut u8> {
        Ok(self.space.borrow_mut().as_mut_ptr())
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("{:?}", args);
    let mut all_cmp = HashMap::new();
    let cmp: PageKeyCmpRc = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
    all_cmp.insert(1u32, cmp.clone());

    let fstore = MockPageStorage::new();
    Storage::init(&fstore, &StorageParams::default())?;

    let mut store = Storage::open(&fstore, &all_cmp)?;

    let full_time_begin = Instant::now();
    for key in 0..args.count {
        let cur_begin = Instant::now();
        if args.verbose {
            let info = store.info()?;
            for rinfo in info {
                print!("{}", rinfo);
            }
            println!();
        }
        let cur_key_sl = unsafe { any_as_u8_slice(&key) };
        store.insert(1, &cur_key_sl, &cur_key_sl)?;
        {
            let find_res = store.find(1, cur_key_sl)?;
            assert!(find_res.is_some());
        }
        let cur_duration = cur_begin.elapsed();
        let info = store.info()?;
        print!(
            "\rwrite  cur:{}% blocks:{} time:{:?}",
            (100f32 * key as f32) / (args.count as f32),
            info.len(),
            cur_duration
        );
        let _ = std::io::stdout().flush();
    }
    let duration = full_time_begin.elapsed();
    let info = fstore.get_info();
    println!("\n allocations:{}", info.allocations);
    println!(" miss_find:{}", info.stat_miss_find);
    println!(" miss_insert:{}", info.stat_miss_insert);
    println!(" total elapsed:{:?}", duration);
    Ok(())
}
