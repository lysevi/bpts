use std::collections::HashMap;

use crate::prelude::*;
use crate::transaction::Transaction;

/*Page:
0:[HEAD]
space:{
    Trans1,Trans1.1,TransList,Trans2.1,TransList
    ....
    Data4,Data3,Data2,Data1
}
TransList: count_u32 [offset of transactions]
*/
#[repr(C, packed)]
struct Header {
    pub id: u32,
    pub trans_list_offset: u32,
    pub free_space_head: u32,
}

const HEADER_SIZE: usize = std::mem::size_of::<Header>();

#[repr(C, packed)]
struct DataList {}

struct Page<'a> {
    buffer: &'a mut [u8],
    space: *mut u8,
    hdr: *mut Header,
    trans: HashMap<u32, Transaction>,
}

impl<'a> Page<'a> {
    pub fn from_buf(buffer: &mut [u8]) -> Result<Page> {
        let result: Page;

        let h = buffer.as_mut_ptr() as *mut Header;
        let space = unsafe { buffer.as_mut_ptr().add(HEADER_SIZE) };
        let t: HashMap<u32, Transaction> = unsafe {
            if (*h).trans_list_offset == 0 {
                HashMap::new()
            } else {
                let mut ptr = space.add((*h).trans_list_offset as usize);
                let count = ptr as *const u32;
                let mut transes = HashMap::new();

                for _i in 0..*count {
                    ptr = ptr.add(std::mem::size_of::<u32>());
                    let offset = ptr as *const u32;
                    let cur_trans_offset = space.add(*offset as usize);
                    let cur_trans = Transaction::from_buffer(cur_trans_offset, *offset);
                    transes.insert(cur_trans.tree_id(), cur_trans);
                }

                transes
            }
        };
        result = Page {
            buffer: buffer,
            hdr: h,
            trans: t,
            space: space,
        };

        return Ok(result);
    }

    pub fn save_trans(&mut self, t: Transaction) {
        unsafe {
            let mut free_space_head = (*self.hdr).free_space_head;
            let mut target = t;
            let offset = free_space_head;
            let ptr = self.space.add(offset as usize);
            let writed_bytes = target.save_to(ptr, offset);
            free_space_head += writed_bytes;

            self.trans.insert(target.tree_id(), target);

            let trans_list_offset = free_space_head;

            let mut ptr = self.space.add(free_space_head as usize);

            let count_ptr = ptr as *mut u32;
            let count = self.trans.len() as u32;
            std::ptr::copy(&count, count_ptr, 1);
            for trans in self.trans.iter() {
                ptr = ptr.add(std::mem::size_of::<u32>());
                let offset_ptr = ptr as *mut u32;
                let value = trans.1.offset();
                std::ptr::copy(&value, offset_ptr, 1);
            }

            let offset = std::mem::size_of::<u32>() * (self.trans.len() + 1);
            free_space_head += offset as u32;

            (*self.hdr).free_space_head = free_space_head;
            (*self.hdr).trans_list_offset = trans_list_offset;
        }

        //self.trans = Some(transes);
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

    pub fn transaction(&self, tree_id: u32) -> Option<Transaction> {
        if let Some(v) = self.trans.get(&tree_id) {
            return Some(v.clone());
        }
        None
    }

    pub fn trees_count(&self) -> usize {
        self.trans.len()
    }

    pub fn is_full() -> bool {
        todo!()
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
            let t = Transaction::new(3, 7);
            assert!(page.transaction(7).is_none());
            page.save_trans(t);
            assert_eq!(page.transaction(7).unwrap().rev(), 3);
        }
        {
            let mut page = Page::from_buf(&mut b)?;
            assert_eq!(page.transaction(7).unwrap().rev(), 3);
            assert_eq!(page.transaction(7).unwrap().tree_id(), 7);
            assert_eq!(page.trees_count(), 1);

            let t = Transaction::new(1, 8);
            assert!(!t.is_readonly());
            page.save_trans(t);
            assert_eq!(page.transaction(8).unwrap().rev(), 1);
            assert!(page.transaction(8).unwrap().is_readonly());
            assert_eq!(page.transaction(8).unwrap().tree_id(), 8);
            assert_eq!(page.trees_count(), 2);
        }

        {
            let mut page = Page::from_buf(&mut b)?;
            assert_eq!(page.trees_count(), 2);
            assert_eq!(page.transaction(7).unwrap().rev(), 3);
            assert_eq!(page.transaction(7).unwrap().tree_id(), 7);

            assert_eq!(page.transaction(8).unwrap().rev(), 1);
            assert_eq!(page.transaction(8).unwrap().tree_id(), 8);

            let t = Transaction::new(2, 8);
            page.save_trans(t);
        }

        {
            let mut page = Page::from_buf(&mut b)?;
            assert_eq!(page.trees_count(), 2);
            assert_eq!(page.transaction(7).unwrap().rev(), 3);
            assert_eq!(page.transaction(7).unwrap().tree_id(), 7);

            assert_eq!(page.transaction(8).unwrap().rev(), 2);
            assert_eq!(page.transaction(8).unwrap().tree_id(), 8);
        }
        return Ok(());
    }
}
