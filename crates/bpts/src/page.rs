use crate::prelude::*;
use crate::transaction::Transaction;
use crate::utils;

#[repr(C, packed)]
struct Header {
    pub id: u32,
    pub trans_offset: usize,
    pub trans_size: usize,
}

const HEADER_SIZE: usize = std::mem::size_of::<Header>();

#[repr(C, packed)]
struct DataList {}

struct Page<'a> {
    buffer: &'a mut [u8],
    hdr: *mut Header,

    trans: Option<Transaction>,
}

impl<'a> Page<'a> {
    pub fn from_buf(buffer: &mut [u8]) -> Result<Page> {
        let result: Page;

        let h = buffer.as_mut_ptr() as *mut Header;
        let t: Option<Transaction> = unsafe {
            if (*h).trans_offset == 0 {
                None
            } else {
                let ptr = buffer.as_mut_ptr();
                let trans_offset = ptr.add((*h).trans_offset);
                Some(Transaction::from_buffer(trans_offset))
            }
        };
        result = Page {
            buffer: buffer,
            hdr: h,
            trans: t,
        };

        return Ok(result);
    }

    pub fn save_trans(&mut self, t: Transaction) {
        unsafe {
            if self.trans.is_none() {
                let offset = HEADER_SIZE;
                let buff_ptr = self.buffer.as_mut_ptr().add(offset);
                let writed_bytes = t.save_to(buff_ptr);
                self.trans = Some(t);
                (*self.hdr).trans_size = writed_bytes;
                (*self.hdr).trans_offset = offset;
            } else {
                todo!();
            }
        }
    }

    pub fn get_id(&self) -> u32 {
        let result = unsafe { (*self.hdr).id };
        return result;
    }

    pub fn set_id(&mut self, i: u32) {
        unsafe {
            (*self.hdr).id = i;
        }
    }

    pub fn transaction(&self) -> Option<Transaction> {
        match &self.trans {
            Some(t) => Some(t.clone()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page::Page;
    use crate::prelude::Result;
    use crate::transaction::Transaction;

    #[test]
    fn page_from_buffer() -> Result<()> {
        let mut b = [0u8; 1024];
        {
            let mut page = Page::from_buf(&mut b)?;
            assert_eq!(page.get_id(), 0);
            page.set_id(777);
        }
        {
            let page2 = Page::from_buf(&mut b)?;
            assert_eq!(page2.get_id(), 777);
        }
        {
            let mut page = Page::from_buf(&mut b)?;
            let t = Transaction::new(3);
            assert!(page.transaction().is_none());
            page.save_trans(t);
            assert_eq!(page.transaction().unwrap().id(), 3);
        }
        {
            let page = Page::from_buf(&mut b)?;
            assert_eq!(page.transaction().unwrap().id(), 3);
        }
        return Ok(());
    }
}
