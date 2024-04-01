use crate::Result;

#[derive(Clone, Copy)]
pub struct FreeListHeader {
    len: u32,
}

//TODO! bitmap
#[repr(C, packed)]
pub struct FreeList {
    buffer: *mut u8,
    hdr: FreeListHeader,
}

const FREE_LIST_HEADER_SIZE: usize = std::mem::size_of::<FreeListHeader>();

impl FreeList {
    pub fn calc_full_size(databuffsize: u32, cluster_size: u16) -> u32 {
        let fsize = FREE_LIST_HEADER_SIZE as f32 + databuffsize as f32 / cluster_size as f32;
        return fsize.ceil() as u32;
    }

    pub unsafe fn open(buffer: *mut u8) -> FreeList {
        let hdr = (buffer as *mut FreeListHeader).read();
        return FreeList {
            hdr: hdr,
            buffer: buffer.add(FREE_LIST_HEADER_SIZE),
        };
    }

    pub unsafe fn init(buffer: *mut u8, databuffsize: u32, cluster_size: u16) -> FreeList {
        let len =
            FreeList::calc_full_size(databuffsize, cluster_size) - FREE_LIST_HEADER_SIZE as u32;
        let hdr = FreeListHeader { len };
        (buffer as *mut FreeListHeader).write(hdr.clone());
        let space = buffer.add(FREE_LIST_HEADER_SIZE);
        for i in 0..hdr.len {
            space.add(i as usize).write(0u8);
        }
        return FreeList {
            hdr: hdr,
            buffer: space,
        };
    }

    pub unsafe fn set(&mut self, i: usize, val: bool) -> Result<()> {
        if i > self.hdr.len as usize {
            return Err(crate::Error::Fail("out of bounds".to_owned()));
        }
        let f = if val { 1u8 } else { 0u8 };
        self.buffer.add(i).write(f);
        Ok(())
    }

    pub fn len(&self) -> usize {
        (self.hdr.len) as usize
    }

    pub unsafe fn get(&self, i: usize) -> Result<bool> {
        if i > self.hdr.len as usize {
            return Err(crate::Error::Fail("out of bounds".to_owned()));
        }

        let f: u8 = self.buffer.add(i).read();
        if f == 1 {
            return Ok(true);
        }
        return Ok(false);
    }

    pub unsafe fn is_full(&self) -> bool {
        for index in 0..(self.hdr.len as usize) {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                return false;
            }
        }
        true
    }

    pub unsafe fn free_clusters(&self) -> usize {
        let mut res = 0;
        for index in 0..(self.hdr.len as usize) {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                res += 1;
            }
        }
        res
    }

    pub unsafe fn get_region_top(&self, i: usize) -> Option<usize> {
        for index in 0..(self.hdr.len as usize) {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                let mut all_is_free = true;
                for j in index..(index + i) {
                    let v: u8 = self.buffer.add(j).read();
                    if v != 0 {
                        all_is_free = false;
                        break;
                    }
                }
                if all_is_free {
                    return Some(index);
                }
            }
        }
        None
    }

    pub unsafe fn get_region_bottom(&self, i: usize) -> Option<usize> {
        for index in (i..(self.hdr.len as usize)).rev() {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                let mut all_is_free = true;

                let from = index + 1;
                let to = index - i + 1;
                for j in (to..from).rev() {
                    let v: u8 = self.buffer.add(j).read();
                    if v != 0 {
                        all_is_free = false;
                        break;
                    }
                }
                if all_is_free {
                    return Some(index - i + 1);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::FreeList;

    #[test]
    fn freelist() -> crate::Result<()> {
        const BUFFERLEN: usize = 100;
        let buffsize = FreeList::calc_full_size(BUFFERLEN as u32, 1);
        let mut buffer = vec![0u8; buffsize as usize + 10];
        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            buffer[pos] = i as u8;
        }

        let mut fl = unsafe { FreeList::init(buffer.as_mut_ptr(), BUFFERLEN as u32, 1) };

        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            buffer[pos] = i as u8;
        }

        unsafe {
            assert!(fl.set((buffsize * 2) as usize, true).is_err());

            assert!(!fl.set(1, true).is_err());

            assert!(fl.get(1).unwrap());

            assert!(!fl.set(2, true).is_err());
            assert!(!fl.set(6, true).is_err());

            assert_eq!(fl.get_region_top(3).unwrap(), 3);

            assert!(!fl.set(99, true).is_err());
            assert!(!fl.set(98, true).is_err());
            assert!(!fl.set(94, true).is_err());
            assert_eq!(fl.get_region_bottom(3).unwrap(), 95);
        }
        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            assert_eq!(buffer[pos], i as u8);
        }
        Ok(())
    }
}
