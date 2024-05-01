use crate::Error;
use crate::Result;

pub(super) struct Buffer {
    pub data: Vec<u8>,
    pub pos: usize,
}

impl Buffer {
    pub fn new(size: usize) -> Self {
        let mut b = Vec::with_capacity(size);
        b.resize(size, 0u8);
        Buffer { data: b, pos: 0 }
    }

    fn write_bool(&mut self, v: bool) -> Result<()> {
        if self.pos < self.data.len() {
            self.data[self.pos] = if v { 1 } else { 0 };
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    fn write_u8(&mut self, v: u8) -> Result<()> {
        if self.pos < self.data.len() {
            self.data[self.pos] = v;
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    fn write_u16(&mut self, v: u16) -> Result<()> {
        if self.pos + 1 < self.data.len() {
            self.data[self.pos] = ((v & 0xff00) >> 8) as u8;
            self.pos += 1;
            self.data[self.pos] = (v & 0x00ff) as u8;
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    fn write_u32(&mut self, v: u32) -> Result<()> {
        if self.pos + 3 < self.data.len() {
            self.data[self.pos] = ((v & 0xff00_0000) >> 24) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x00ff_0000) >> 16) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_ff00) >> 8) as u8;
            self.pos += 1;
            self.data[self.pos] = (v & 0x0000_00ff) as u8;
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    fn write_u64(&mut self, v: u64) -> Result<()> {
        if self.pos + 7 < self.data.len() {
            self.data[self.pos] = ((v & 0xff00_0000_0000_0000) >> 56) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x00ff_0000_0000_0000) >> 48) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_ff00_0000_0000) >> 40) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_00ff_0000_0000) >> 32) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_0000_ff00_0000) >> 24) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_0000_00ff_0000) >> 16) as u8;
            self.pos += 1;
            self.data[self.pos] = ((v & 0x0000_0000_0000_ff00) >> 8) as u8;
            self.pos += 1;
            self.data[self.pos] = (v & 0x0000_0000_0000_00ff) as u8;
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    fn read_bool(&self, seek: usize) -> Result<bool> {
        return Ok(self.data[seek] == 1);
    }

    fn read_u8(&self, seek: usize) -> Result<u8> {
        return Ok(self.data[seek]);
    }

    fn read_u16(&self, seek: usize) -> Result<u16> {
        let a = self.data[seek] as u16;
        let b = self.data[seek + 1] as u16;
        return Ok(a << 8 | b);
    }

    fn read_u32(&self, seek: usize) -> Result<u32> {
        let a = self.data[seek] as u32;
        let b = self.data[seek + 1] as u32;
        let c = self.data[seek + 2] as u32;
        let d = self.data[seek + 3] as u32;
        Ok((a << 24) | (b << 16) | (c << 8) | d)
    }

    fn read_u64(&self, seek: usize) -> Result<u64> {
        let a = self.data[seek] as u64;
        let b = self.data[seek + 1] as u64;
        let c = self.data[seek + 2] as u64;
        let d = self.data[seek + 3] as u64;

        let e = self.data[seek + 4] as u64;
        let f = self.data[seek + 5] as u64;
        let g = self.data[seek + 6] as u64;
        let h = self.data[seek + 7] as u64;
        Ok((a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) | (f << 16) | (g << 8) | h)
    }
}

#[cfg(test)]
mod tests {
    use crate::Result;

    use super::Buffer;
    #[test]
    fn rw_bool() -> Result<()> {
        let mut b = Buffer::new(3);
        b.write_bool(true)?;
        b.write_bool(false)?;
        b.write_bool(true)?;
        let err = b.write_bool(false);
        assert!(err.is_err());
        assert!(b.read_bool(0)?);
        assert!(!b.read_bool(1)?);
        assert!(b.read_bool(2)?);
        Ok(())
    }

    #[test]
    fn rw_u8() -> Result<()> {
        let mut b = Buffer::new(2);
        b.write_u8(1)?;
        b.write_u8(2)?;
        let err = b.write_u8(2);
        assert!(err.is_err());
        assert_eq!(b.read_u8(0)?, 1u8);
        assert_eq!(b.read_u8(1)?, 2u8);
        Ok(())
    }

    #[test]
    fn rw_u16() -> Result<()> {
        let mut b = Buffer::new(std::mem::size_of::<u16>() * 3);
        b.write_u16(std::u16::MAX)?;
        b.write_u16(0u16)?;
        b.write_u16(std::u16::MIN)?;
        let err = b.write_u16(2);
        assert!(err.is_err());
        assert_eq!(b.read_u16(0)?, std::u16::MAX);
        assert_eq!(b.read_u16(2)?, 0u16);
        assert_eq!(b.read_u16(4)?, std::u16::MIN);
        Ok(())
    }

    #[test]
    fn rw_u32() -> Result<()> {
        let mut b = Buffer::new(std::mem::size_of::<u32>() * 3);
        b.write_u32(std::u32::MAX)?;
        b.write_u32(0u32)?;
        b.write_u32(std::u32::MIN)?;
        let err = b.write_u32(2);
        assert!(err.is_err());
        assert_eq!(b.read_u32(0)?, std::u32::MAX);
        assert_eq!(b.read_u32(4)?, 0u32);
        assert_eq!(b.read_u32(8)?, std::u32::MIN);
        Ok(())
    }

    #[test]
    fn rw_u64() -> Result<()> {
        let mut b = Buffer::new(std::mem::size_of::<u64>() * 3);
        b.write_u64(std::u64::MAX)?;
        b.write_u64(0u64)?;
        b.write_u64(std::u64::MIN)?;
        let err = b.write_u64(2);
        assert!(err.is_err());
        assert_eq!(b.read_u64(0)?, std::u64::MAX);
        assert_eq!(b.read_u64(8)?, 0u64);
        assert_eq!(b.read_u64(16)?, std::u64::MIN);
        Ok(())
    }
}
