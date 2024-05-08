use super::buffer::Buffer;
use super::flat_storage::FlatStorage;
use crate::types::Id;
use crate::utils::any_as_u8_slice;
use crate::Result;

use std::cell::RefCell;
use std::fs::File;
use std::io::prelude::*;

pub struct BufFileStorage {
    buffer: RefCell<Buffer>,
    file: RefCell<File>,
}

impl BufFileStorage {
    pub fn new(filename: &str, buffsize: usize) -> Result<Self> {
        let f = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(filename);

        Ok(BufFileStorage {
            buffer: RefCell::new(Buffer::new(buffsize)),
            file: RefCell::new(f.unwrap()),
        })
    }

    pub fn open(filename: &str, buffsize: usize) -> Result<Self> {
        let f = File::options()
            .read(true)
            .append(true)
            .create(false)
            .open(filename);

        Ok(BufFileStorage {
            buffer: RefCell::new(Buffer::new(buffsize)),
            file: RefCell::new(f.unwrap()),
        })
    }

    fn write_slice(&self, value: &[u8]) -> Result<()> {
        let mut file = self.file.borrow_mut();
        let state = file.write(value);

        if state.is_err() {
            return Err(crate::Error::IO(state.err().unwrap()));
        }
        Ok(())
    }

    fn read<T, const SIZE: usize>(&self, seek: std::io::SeekFrom) -> Result<T> {
        let mut file = self.file.borrow_mut();
        let state = file.seek(seek);
        if state.is_err() {
            return Err(crate::Error::IO(state.err().unwrap()));
        }
        let mut out = [0u8; SIZE];
        let state = file.read(&mut out);
        if state.is_err() {
            return Err(crate::Error::IO(state.err().unwrap()));
        }
        return Ok(unsafe { (out.as_ptr() as *const T).read() });
    }
}

impl FlatStorage for BufFileStorage {
    fn flush(&self) -> Result<()> {
        let mut buf_ref = self.buffer.borrow_mut();
        let sl = buf_ref.as_slice();
        if sl.len() > 0 {
            let state = self.file.borrow_mut().write(sl);
            if state.is_err() {
                return Err(crate::Error::IO(state.err().unwrap()));
            }
        }
        buf_ref.reset();

        let state = self.file.borrow().sync_all();
        if state.is_err() {
            return Err(crate::Error::IO(state.err().unwrap()));
        }
        Ok(())
    }
    fn close(&self) -> Result<()> {
        self.flush()
    }

    fn params_write(&self, h: &crate::prelude::StorageParams) -> Result<()> {
        let ptr = unsafe { any_as_u8_slice(h) };
        return self.write_slice(ptr);
    }

    fn params_read(&self) -> Result<crate::prelude::StorageParams> {
        const STORAGE_PARAMS_SIZE: usize = std::mem::size_of::<crate::prelude::StorageParams>();
        return self.read::<crate::prelude::StorageParams, STORAGE_PARAMS_SIZE>(
            std::io::SeekFrom::Start(0),
        );
    }

    fn header_write(&self, h: &super::store::StorageHeader) -> Result<()> {
        let ptr = unsafe { any_as_u8_slice(h) };
        return self.write_slice(ptr);
    }

    fn header_read(&self) -> Result<super::store::StorageHeader> {
        const STORAGE_HEADER_SIZE: usize = std::mem::size_of::<super::store::StorageHeader>();
        return self.read::<super::store::StorageHeader, STORAGE_HEADER_SIZE>(
            std::io::SeekFrom::End(-(STORAGE_HEADER_SIZE as i64)),
        );
    }

    fn size(&self) -> usize {
        let mut file = self.file.borrow_mut();
        let pos = file.seek(std::io::SeekFrom::End(0)).unwrap();
        return pos as usize + self.buffer.borrow().size();
    }

    fn write_id(&self, v: crate::types::Id) -> Result<()> {
        return self.write_u32(v.0);
    }

    fn write_bool(&self, v: bool) -> Result<()> {
        let res = self.buffer.borrow_mut().write_bool(v);
        if res.is_err() {
            self.flush()?;
            return self.buffer.borrow_mut().write_bool(v);
        }
        return Ok(());
    }

    fn write_u8(&self, v: u8) -> Result<()> {
        let res = self.buffer.borrow_mut().write_u8(v);
        if res.is_err() {
            self.flush()?;
            return self.buffer.borrow_mut().write_u8(v);
        }
        return Ok(());
    }

    fn write_u16(&self, v: u16) -> Result<()> {
        let res = self.buffer.borrow_mut().write_u16(v);
        if res.is_err() {
            self.flush()?;
            return self.buffer.borrow_mut().write_u16(v);
        }
        return Ok(());
    }

    fn write_u32(&self, v: u32) -> Result<()> {
        let res = self.buffer.borrow_mut().write_u32(v);
        if res.is_err() {
            self.flush()?;
            return self.buffer.borrow_mut().write_u32(v);
        }
        return Ok(());
    }

    fn write_u64(&self, v: u64) -> Result<()> {
        let res = self.buffer.borrow_mut().write_u64(v);
        if res.is_err() {
            self.flush()?;
            return self.buffer.borrow_mut().write_u64(v);
        }
        return Ok(());
    }

    fn read_id(&self, seek: usize) -> Result<crate::types::Id> {
        let r = self.read_u32(seek)?;
        Ok(Id(r))
    }

    fn read_bool(&self, seek: usize) -> Result<bool> {
        const SIZE: usize = std::mem::size_of::<u8>();
        let v = self.read::<u8, SIZE>(std::io::SeekFrom::Start(seek as u64))?;
        return Ok(v == 1);
    }

    fn read_u8(&self, seek: usize) -> Result<u8> {
        const SIZE: usize = std::mem::size_of::<u8>();
        let v = self.read::<u8, SIZE>(std::io::SeekFrom::Start(seek as u64))?;
        return Ok(v);
    }

    fn read_u16(&self, seek: usize) -> Result<u16> {
        const SIZE: usize = std::mem::size_of::<u16>();
        let v = self.read::<u16, SIZE>(std::io::SeekFrom::Start(seek as u64))?;
        return Ok(v);
    }

    fn read_u32(&self, seek: usize) -> Result<u32> {
        const SIZE: usize = std::mem::size_of::<u32>();
        let v = self.read::<u32, SIZE>(std::io::SeekFrom::Start(seek as u64))?;
        return Ok(v);
    }

    fn read_u64(&self, seek: usize) -> Result<u64> {
        const SIZE: usize = std::mem::size_of::<u64>();
        let v = self.read::<u64, SIZE>(std::io::SeekFrom::Start(seek as u64))?;
        return Ok(v);
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use storage::KeyCmp;

    use crate::{prelude::Storage, types::Id, utils::any_as_u8_slice, *};

    use self::{
        prelude::{FlatStorage, StorageParams},
        storage::{store::StorageHeader, MAGIC_HEADER},
    };

    use super::BufFileStorage;
    extern crate tempfile;

    struct MockStorageKeyCmp {}

    impl MockStorageKeyCmp {
        fn new() -> MockStorageKeyCmp {
            MockStorageKeyCmp {}
        }
    }

    impl KeyCmp for MockStorageKeyCmp {
        fn compare(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
            if key1.len() != key2.len() {
                panic!()
            }
            key1.cmp(key2)
        }
    }

    #[test]
    fn read_write() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let pathbuff = tempdir.path().join("flat_file_storage_test");
        let filename = pathbuff.to_str().unwrap();
        if std::path::Path::new(&filename).is_file() {
            println!("removing {:?}", filename);
            std::fs::remove_file(filename).unwrap();
        }
        let storage = BufFileStorage::new(&filename, 1024)?;
        let sparams = StorageParams::default();
        storage.params_write(&sparams)?;
        let readed = storage.params_read()?;
        unsafe {
            let origin = utils::any_as_u8_slice(&sparams);
            let checked = utils::any_as_u8_slice(&readed);
            assert_eq!(origin, checked);
        }

        let header = StorageHeader {
            is_closed: 1,
            magic: MAGIC_HEADER,
            offset: 112233,
        };
        storage.header_write(&header)?;
        let readed_header = storage.header_read()?;
        unsafe {
            let origin = utils::any_as_u8_slice(&header);
            let checked = utils::any_as_u8_slice(&readed_header);
            assert_eq!(origin, checked);
        }

        let bool_offset = storage.size();
        storage.write_bool(true)?;
        let u8offset = storage.size();
        storage.write_u8(11u8)?;

        let u16offset = storage.size();
        storage.write_u16(std::u16::MAX - 1)?;

        let u32offset = storage.size();
        storage.write_u32(std::u32::MAX - 1)?;

        let u64offset = storage.size();
        storage.write_u64(std::u64::MAX - 1)?;

        let idoffset = storage.size();
        storage.write_id(Id(std::u32::MAX - 2))?;
        storage.flush()?;

        let readed_bool = storage.read_bool(bool_offset)?;
        let readed_u8 = storage.read_u8(u8offset)?;
        let readed_u16 = storage.read_u16(u16offset)?;
        let readed_u32 = storage.read_u32(u32offset)?;
        let readed_u64 = storage.read_u64(u64offset)?;
        let readed_id = storage.read_id(idoffset)?;

        assert_eq!(readed_bool, true);
        assert_eq!(readed_u8, 11u8);
        assert_eq!(readed_u16, std::u16::MAX - 1);

        assert_eq!(readed_u32, std::u32::MAX - 1);
        assert_eq!(readed_u64, std::u64::MAX - 1);
        assert_eq!(readed_id, Id(std::u32::MAX - 2));
        Ok(())
    }

    #[test]
    fn db() -> Result<()> {
        let tempdir = tempfile::tempdir().unwrap();
        let pathbuff = tempdir.path().join("flat_file_storage_test");
        let filename = pathbuff.to_str().unwrap();
        if std::path::Path::new(&filename).is_file() {
            println!("removing {:?}", filename);
            std::fs::remove_file(filename).unwrap();
        }
        let fstorage = Rc::new(RefCell::new(BufFileStorage::new(&filename, 1024 * 1024)?));

        let mut all_cmp: HashMap<u32, Rc<RefCell<dyn KeyCmp>>> = HashMap::new();
        let cmp = Rc::new(RefCell::new(MockStorageKeyCmp::new()));
        all_cmp.insert(1u32, cmp);

        let params = StorageParams::default();
        let mut storage = Storage::new(fstorage.clone(), &params, all_cmp)?;
        let max_key = 100;
        let mut all_keys = Vec::new();
        for key in 0..max_key {
            println!("insert {}", key);

            all_keys.push(key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            let tr = storage.begin_transaction()?;
            storage.insert(tr, 1, &cur_key_sl, &cur_key_sl)?;
            storage.commit_transaction(tr)?;
            // {
            //     let find_res = storage.find(1, cur_key_sl)?;
            //     assert!(find_res.is_some());
            //     let value = &find_res.unwrap()[..];
            //     assert_eq!(value, cur_key_sl)
            // }

            // for search_key in 0..key {
            //     println!("read {}", search_key);
            //     let key_sl = unsafe { any_as_u8_slice(&search_key) };
            //     let find_res = storage.find(1, key_sl)?;
            //     assert!(find_res.is_some());
            //     let value = &find_res.unwrap()[..];
            //     assert_eq!(value, key_sl)
            // }
        }

        for key in all_keys.iter() {
            println!("read {}", key);
            let key_sl = unsafe { any_as_u8_slice(key) };
            let find_res = storage.find(1, key_sl)?;
            assert!(find_res.is_some());
            let value = &find_res.unwrap()[..];
            assert_eq!(value, key_sl)
        }

        while all_keys.len() > 0 {
            let key = all_keys[0];
            all_keys.remove(0);

            //println!("remove {}", key);
            let cur_key_sl = unsafe { any_as_u8_slice(&key) };
            let str_before = storage.dump_tree(1, String::from("before"));
            storage.remove(1, &cur_key_sl)?;
            let str_after = storage.dump_tree(1, String::from("after"));

            //crate::tree::debug::print_states(&[&str_before, &str_after]);
            {
                let find_res = storage.find(1, cur_key_sl)?;
                assert!(find_res.is_none());
            }

            for search_key in all_keys.iter() {
                //println!("read {}", search_key);
                let key_sl = unsafe { any_as_u8_slice(search_key) };
                let find_res = storage.find(1, key_sl)?;
                if find_res.is_none() {
                    crate::tree::debug::print_states(&[&str_before, &str_after]);
                }
                assert!(find_res.is_some());
                let value = &find_res.unwrap()[..];
                assert_eq!(value, key_sl)
            }
        }

        let mut hdr = fstorage.borrow().header_read()?;
        assert!(hdr.is_closed == 0);
        storage.close()?;
        hdr = fstorage.borrow().header_read()?;
        assert!(hdr.is_closed == 1);
        println!("size: {}kb", fstorage.borrow().size() as f32 / 1024f32);
        Ok(())
    }
}
