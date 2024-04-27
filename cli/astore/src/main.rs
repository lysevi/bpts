extern crate tempfile;

use clap::Parser;

use std::{cell::RefCell, collections::HashMap, io::Write, path::PathBuf, rc::Rc, time::Instant};

use bpts::{
    prelude::*,
    storage::{file_storage::FileStorage, store::StorageHeader},
    types::{Id, SingleElementStore},
    utils::any_as_u8_slice,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// verbose output
    #[arg(long, default_value_t = false)]
    quiet: bool,

    /// data count
    #[arg(short, long, default_value_t = 10000)]
    count: i32,

    // use in-memory storage
    #[arg(short, long, default_value_t = false)]
    memstorage: bool,

    // use in-memory storage
    #[arg(short, long)]
    filename: Option<PathBuf>,
}

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
            space: RefCell::new(Vec::with_capacity(1024 * 1024 * 20)),
        }
    }
}

impl FlatStorage for MockPageStorage {
    fn close(&self) -> Result<()> {
        Ok(())
    }

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

fn main() -> Result<()> {
    let args = Args::parse();
    println!("{:?}", args);

    let tempdir = tempfile::tempdir().unwrap();
    let pathbuff = if args.filename.is_none() {
        tempdir.path().join("astorage.db")
    } else {
        args.filename.unwrap()
    };
    let filename = pathbuff.to_str().unwrap();
    if std::path::Path::new(&filename).is_file() {
        println!("removing {:?}", filename);
        std::fs::remove_file(filename).unwrap();
    }

    println!("dbfile: {}", filename);
    let mut all_cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>> = HashMap::new();
    let cmp = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
    all_cmp.insert(1u32, cmp);

    let fstore: Rc<RefCell<dyn FlatStorage>> = if args.memstorage {
        Rc::new(RefCell::new(MockPageStorage::new()))
    } else {
        Rc::new(RefCell::new(FileStorage::new(&filename)?))
    };

    let params = StorageParams::default();
    println!("{:?}", params.tree_params);
    let mut storage = Storage::new(fstore.clone(), &params, all_cmp)?;

    let full_time_begin = Instant::now();

    let write_time_begin = Instant::now();
    for key in 0..args.count {
        let cur_begin = Instant::now();
        let cur_key_sl = unsafe { any_as_u8_slice(&key) };
        storage.insert(1, &cur_key_sl, &cur_key_sl)?;

        let cur_duration = cur_begin.elapsed();

        if !args.quiet {
            print!(
                "\rwrite cur:{}% size:{} time:{:?}                ",
                (100f32 * key as f32) / (args.count as f32),
                fstore.borrow().size() / 1024,
                cur_duration
            );
            let _ = std::io::stdout().flush();
        }
    }

    let write_duration = write_time_begin.elapsed();

    if !args.quiet {
        println!("");
    }

    let read_time_begin = Instant::now();
    for key in 0..args.count {
        let cur_begin = Instant::now();

        let cur_key_sl = unsafe { any_as_u8_slice(&key) };

        let find_res = storage.find(1, cur_key_sl)?;
        assert!(find_res.is_some());

        let cur_duration = cur_begin.elapsed();
        if !args.quiet {
            print!(
                "\rread cur:{}% time:{:?}                ",
                (100f32 * key as f32) / (args.count as f32),
                cur_duration
            );
            let _ = std::io::stdout().flush();
        }
    }
    let read_duration = read_time_begin.elapsed();

    let duration = full_time_begin.elapsed();

    println!("");
    println!(" size: {}", fstore.borrow().size() / 1024);
    println!(" total write time: {:?}", write_duration);
    println!(" total read time: {:?}", read_duration);
    println!(" total elapsed: {:?}", duration);
    Ok(())
}
