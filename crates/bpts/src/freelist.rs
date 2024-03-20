use bpts_tree::prelude::Result;

//TODO! bitmap
pub struct FreeList {
    buffer: *mut u8,
    bufflen: u32,
}

impl FreeList {
    pub fn calc_size(buffsize: u32, cluster_size: u16) -> u32 {
        return buffsize / cluster_size as u32;
    }

    pub fn size_for_len(len: u32) -> u32 {
        return len;
    }

    pub fn new(buffer: *mut u8, bufflen: u32) -> FreeList {
        FreeList { buffer, bufflen }
    }

    pub unsafe fn init(&mut self) {
        for i in 0..self.bufflen {
            self.buffer.add(i as usize).write(0u8);
        }
    }

    pub unsafe fn set(&mut self, i: usize, val: bool) -> Result<()> {
        if i > self.bufflen as usize {
            return Err(bpts_tree::types::Error("out of bounds".to_owned()));
        }
        let f = if val { 1u8 } else { 0u8 };
        self.buffer.add(i).write(f);
        Ok(())
    }

    pub fn len(&self) -> usize {
        (self.bufflen) as usize
    }

    pub unsafe fn get(&self, i: usize) -> Result<bool> {
        if i > self.bufflen as usize {
            return Err(bpts_tree::types::Error("out of bounds".to_owned()));
        }

        let f: u8 = self.buffer.add(i).read();
        if f == 1 {
            return Ok(true);
        }
        return Ok(false);
    }

    pub unsafe fn is_full(&self) -> bool {
        for index in 0..(self.bufflen as usize) {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                return false;
            }
        }
        true
    }

    pub unsafe fn free_clusters(&self) -> usize {
        let mut res = 0;
        for index in 0..(self.bufflen as usize) {
            let v: u8 = self.buffer.add(index).read();
            if v == 0 {
                res += 1;
            }
        }
        res
    }

    pub unsafe fn get_region_top(&self, i: usize) -> Option<usize> {
        for index in 0..(self.bufflen as usize) {
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
        for index in (i..(self.bufflen as usize)).rev() {
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
    use bpts_tree::prelude::Result;
    
    #[test]
    fn freelist() -> Result<()> {
        const BUFFERSIZE: usize = 100;
        let mut buffer = vec![0u8; BUFFERSIZE + 10];
        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            buffer[pos] = i as u8;
        }

        let mut fl = FreeList::new(buffer.as_mut_ptr(), BUFFERSIZE as u32);
        unsafe { fl.init() };
        for i in 0..BUFFERSIZE {
            assert_eq!(buffer[i], 0);
        }

        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            buffer[pos] = i as u8;
        }

        unsafe {
            assert!(fl.set(BUFFERSIZE * 2, true).is_err());

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
