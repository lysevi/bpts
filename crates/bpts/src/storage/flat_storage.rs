use super::StorageParams;
use crate::types::Id;
use crate::Result;

pub trait FlatStorage {
    fn header_write(&self, h: &StorageParams) -> Result<()>;
    fn header_read(&self) -> Result<StorageParams>;

    fn size(&self) -> usize;
    fn write_id(&self, v: Id) -> Result<()>;
    fn write_bool(&self, v: bool) -> Result<()>;
    fn write_u8(&self, v: u8) -> Result<()>;
    fn write_u16(&self, v: u16) -> Result<()>;
    fn write_u32(&self, v: u32) -> Result<()>;
    fn write_u64(&self, v: u64) -> Result<()>;

    fn read_id(&self, seek: usize) -> Result<Id>;
    fn read_bool(&self, seek: usize) -> Result<bool>;
    fn read_u8(&self, seek: usize) -> Result<u8>;
    fn read_u16(&self, seek: usize) -> Result<u16>;
    fn read_u32(&self, seek: usize) -> Result<u32>;
    fn read_u64(&self, seek: usize) -> Result<u64>;
}
