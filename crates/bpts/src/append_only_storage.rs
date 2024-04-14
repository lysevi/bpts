use crate::storage::StorageParams;
use crate::Result;

pub trait AppendOnlyStruct {
    fn header_write(&self, h: &StorageParams) -> Result<()>;
    fn header_read(&self) -> Result<Option<*const StorageParams>>;

    fn size(&self) -> usize;
}
