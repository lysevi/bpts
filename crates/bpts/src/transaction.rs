#[repr(C, packed)]
#[derive(Debug, Clone)]
pub struct TransactionHeader {
    rev: u32,
    tree_id: u32,
    size: u32,
}
const TRANSACTION_HEADER_SIZE: u32 = std::mem::size_of::<TransactionHeader>() as u32;

#[derive(Debug, Clone)]
pub struct Transaction {
    header: TransactionHeader,
    buffer: Option<*mut u8>,
    offset: u32,
}

impl Transaction {
    pub fn new(rev: u32, tree_id: u32) -> Transaction {
        let hdr = TransactionHeader {
            rev,
            tree_id,
            size: 0,
        };
        Transaction {
            header: hdr,
            buffer: None,
            offset: 0u32,
        }
    }

    pub unsafe fn from_buffer(buffer: *mut u8, offset: u32) -> Transaction {
        let ptr = buffer as *const TransactionHeader;
        let hdr = std::ptr::read(ptr);
        Transaction {
            header: hdr,
            buffer: Some(buffer),
            offset: offset,
        }
    }

    pub unsafe fn save_to(&mut self, buffer: *mut u8, offset: u32) -> u32 {
        let src_ptr = &self.header as *const TransactionHeader;
        let dest_ptr = buffer as *mut TransactionHeader;
        std::ptr::copy(src_ptr, dest_ptr, 1);
        self.offset = offset;
        return self.size();
    }

    pub fn size(&self) -> u32 {
        return TRANSACTION_HEADER_SIZE;
    }

    pub fn rev(&self) -> u32 {
        self.header.rev
    }

    pub fn tree_id(&self) -> u32 {
        self.header.tree_id
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn set_offset(&mut self, offset: u32, buffer: *mut u8) {
        self.offset = offset;
        self.buffer = Some(buffer);
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::Result;
    use crate::transaction::*;
}
