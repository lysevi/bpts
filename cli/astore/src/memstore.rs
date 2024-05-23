use std::cell::RefCell;

use bpts::{
    storage::{flat_storage::FlatStorage, store::StorageHeader, StorageParams},
    types::{Id, SingleElementStore},
    utils::any_as_u8_slice,
    Result,
};

pub struct MemStorage {
    hdr: RefCell<SingleElementStore<StorageHeader>>,
    params: RefCell<SingleElementStore<StorageParams>>,
    space: RefCell<Vec<u8>>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            params: RefCell::new(SingleElementStore::new()),
            hdr: RefCell::new(SingleElementStore::new()),
            space: RefCell::new(Vec::with_capacity(1024 * 1024 * 20)),
        }
    }
}

impl FlatStorage for MemStorage {
    fn flush(&self) -> Result<()> {
        Ok(())
    }
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
