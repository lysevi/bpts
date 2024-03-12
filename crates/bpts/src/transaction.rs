#[repr(C, packed)]
#[derive(Debug, Clone)]
pub struct TransactionHeader {
    id: u32,
    size: u32,
}
const TRANSACTION_HEADER_SIZE: usize = std::mem::size_of::<TransactionHeader>();

#[derive(Debug, Clone)]
pub struct Transaction {
    header: TransactionHeader,
    buffer: Option<*mut u8>,
}

impl Transaction {
    pub fn new(id: u32) -> Transaction {
        let hdr = TransactionHeader { id, size: 0 };
        Transaction {
            header: hdr,
            buffer: None,
        }
    }

    pub unsafe fn from_buffer(buffer: *mut u8) -> Transaction {
        let ptr = buffer as *const TransactionHeader;
        let hdr = std::ptr::read(ptr);
        Transaction {
            header: hdr,
            buffer: Some(buffer),
        }
    }

    pub unsafe fn save_to(&self, buffer: *mut u8) -> usize {
        let src_ptr = &self.header as *const TransactionHeader;
        let dest_ptr = buffer as *mut TransactionHeader;
        std::ptr::copy(src_ptr, dest_ptr, TRANSACTION_HEADER_SIZE);
        return self.size();
    }

    pub fn size(&self) -> usize {
        return TRANSACTION_HEADER_SIZE;
    }

    pub fn id(&self) -> u32 {
        self.header.id
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::Result;
    use crate::transaction::*;
}
