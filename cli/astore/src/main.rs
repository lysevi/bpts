extern crate tempfile;
mod memstore;
use clap::Parser;

use std::{cell::RefCell, collections::HashMap, io::Write, path::PathBuf, rc::Rc, time::Instant};

use bpts::{
    prelude::*,
    storage::{buffile_storage::BufFileStorage, file_storage::FileStorage, store::StorageHeader},
    types::{Id, SingleElementStore},
    utils::any_as_u8_slice,
};

use memstore::MemStorage;

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

    // use buffered storage
    #[arg(short, long, default_value_t = false)]
    bufstorage: bool,

    // use buffered storage
    #[arg(long, default_value_t = 1024*1024)]
    bufsize: usize,

    // use in-memory storage
    #[arg(short, long)]
    filename: Option<PathBuf>,
}

struct StorageKeyCmp {}

impl StorageKeyCmp {
    fn new() -> StorageKeyCmp {
        StorageKeyCmp {}
    }
}

impl KeyCmp for StorageKeyCmp {
    fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
        key1.cmp(key2)
    }
}

fn main() -> Result<()> {
    let mut args = Args::parse();
    args.bufstorage = true;
    args.count = 500;
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
    let cmp = Rc::new(RefCell::new(StorageKeyCmp::new()));
    all_cmp.insert(1u32, cmp);

    let fstore: Rc<RefCell<dyn FlatStorage>> = if args.memstorage {
        Rc::new(RefCell::new(MemStorage::new()))
    } else {
        if args.bufstorage {
            Rc::new(RefCell::new(BufFileStorage::new(&filename, args.bufsize)?))
        } else {
            Rc::new(RefCell::new(FileStorage::new(&filename)?))
        }
    };

    let params = StorageParams::default();
    println!("{:?}", params.tree_params);
    let mut storage = Storage::new(fstore.clone(), &params, all_cmp)?;

    let full_time_begin = Instant::now();

    let write_time_begin = Instant::now();
    for key in 0..args.count {
        if key == 199 {
            println!("\n {} ", key);
        }
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

        let find_res = storage.find(1, cur_key_sl)?;
        if find_res.is_none() {
            println!("\n {} ", key);
        }
        assert!(find_res.is_some());
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
