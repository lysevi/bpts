use bpts_tree::prelude::Result;

pub struct FreeList {
    buffer: *mut u8,
    bufflen: u32,
}

impl FreeList {
    pub fn new(buffer: *mut u8, bufflen: u32) -> FreeList {
        FreeList { buffer, bufflen }
    }

    pub fn init(&mut self) {
        todo!();
    }

    pub fn set(&mut self, i: usize, val: bool) -> Result<()> {
        todo!();
        Ok(())
    }

    pub fn len(&self) -> usize {
        (self.bufflen * 8) as usize
    }

    pub fn get(&self, i: usize) -> Result<bool> {
        todo!();
        Ok(false)
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

        fl.init();
        for i in 0..BUFFERSIZE {
            assert_eq!(buffer[i], 0);
        }

        assert!(fl.set(BUFFERSIZE * 2, true).is_err());

        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            assert_eq!(buffer[pos], i as u8);
        }
        Ok(())
    }
}
