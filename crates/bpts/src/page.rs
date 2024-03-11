use crate::prelude::*;

#[repr(C, packed)]
struct Header {
    pub id: u32,
}

struct Page {
    hdr: Header,
}

impl Page {
    pub fn from_buf(buffer: &[u8]) -> Result<Page> {
        let hdr: Header = unsafe { std::ptr::read(buffer.as_ptr() as *const _) };

        return Ok(Page { hdr: hdr });
    }

    pub fn get_id(&self) -> u32 {
        self.hdr.id
    }

    pub fn set_id(&mut self, i: u32) {
        self.hdr.id = i;
    }

    pub fn save_to_buf(&self, buf: &mut [u8]) -> crate::prelude::Result<()> {
        unsafe {
            let tmp = std::ptr::read(&self.hdr);
            std::ptr::write::<Header>(buf.as_mut_ptr() as *mut Header, tmp);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::page::Page;
    use crate::prelude::Result;

    #[test]
    fn page_from_buffer() -> Result<()> {
        let mut b = [0u8; 1024];
        let mut page = Page::from_buf(&b)?;
        assert_eq!(page.get_id(), 0);
        page.set_id(777);
        page.save_to_buf(&mut b)?;

        let mut page2 = Page::from_buf(&b)?;
        assert_eq!(page2.get_id(), 777);
        assert!(true);
        return Ok(());
    }
}
