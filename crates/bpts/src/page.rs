use crate::prelude::*;

#[repr(C, packed)]
struct Header {
    pub id: u32,
}

struct Page {
    hdr: *mut Header,
}

impl Page {
    pub fn from_buf(buffer: &mut [u8]) -> Result<Page> {
        let result: Page;

        let h = buffer.as_mut_ptr() as *mut Header;
        result = Page { hdr: h };

        return Ok(result);
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
}

#[cfg(test)]
mod tests {
    use crate::page::Page;
    use crate::prelude::Result;

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
        return Ok(());
    }
}
