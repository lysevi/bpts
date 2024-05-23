use crate::Error;
use crate::Result;

macro_rules! create_write_method {
    ($fn_name:ident, $type_name:ty) => {
        pub fn $fn_name(&mut self, v: $type_name) -> Result<()> {
            let sz = std::mem::size_of_val(&v);
            if self.pos + sz - 1 < self.data.len() {
                let mut mask = 0xff as $type_name;
                for i in 0..sz {
                    self.data[self.pos] = ((v & mask) >> i * 8) as u8;
                    self.pos += 1;
                    mask = mask << (8 as $type_name);
                }
                return Ok(());
            }
            Err(Error::IsFull)
        }
    };
}

macro_rules! create_read_method {
    ($fn_name:ident, $type_name:ty) => {
        pub fn $fn_name(&self, seek: usize) -> Result<$type_name> {
            let sz = std::mem::size_of::<$type_name>();
            let mut result: $type_name = 0;

            for i in 0..sz {
                result = result | ((self.data[seek + i] as $type_name) << (8 * i));
            }
            return Ok(result);
        }
    };
}

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

    pub fn size(&self) -> usize {
        return self.pos;
    }

    pub fn as_slice(&self) -> &[u8] {
        return &self.data[0..self.pos];
    }

    pub fn reset(&mut self) {
        self.pos = 0;
    }

    //create_write_method!(write_u8, u8);

    pub fn write_bool(&mut self, v: bool) -> Result<()> {
        let val = if v { 1 } else { 0 };
        return self.write_u8(val);
    }

    pub fn write_u8(&mut self, v: u8) -> Result<()> {
        if self.pos < self.data.len() {
            self.data[self.pos] = v;
            self.pos += 1;
            return Ok(());
        }
        Err(Error::IsFull)
    }

    create_write_method!(write_u16, u16);
    create_write_method!(write_u32, u32);
    create_write_method!(write_u64, u64);

    fn read_bool(&self, seek: usize) -> Result<bool> {
        return Ok(self.read_u8(seek)? == 1);
    }

    create_read_method!(read_u8, u8);
    create_read_method!(read_u16, u16);
    create_read_method!(read_u32, u32);
    create_read_method!(read_u64, u64);
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
        b.write_u16(std::u16::MAX - 1)?;
        b.write_u16(0u16)?;
        b.write_u16(std::u16::MIN)?;
        let err = b.write_u16(2);
        assert!(err.is_err());
        assert_eq!(b.read_u16(0)?, std::u16::MAX - 1);
        assert_eq!(b.read_u16(2)?, 0u16);
        assert_eq!(b.read_u16(4)?, std::u16::MIN);
        Ok(())
    }

    #[test]
    fn rw_u32() -> Result<()> {
        let mut b = Buffer::new(std::mem::size_of::<u32>() * 3);
        b.write_u32(std::u32::MAX - 1)?;
        b.write_u32(0u32)?;
        b.write_u32(std::u32::MIN)?;
        let err = b.write_u32(2);
        assert!(err.is_err());
        assert_eq!(b.read_u32(0)?, std::u32::MAX - 1);
        assert_eq!(b.read_u32(4)?, 0u32);
        assert_eq!(b.read_u32(8)?, std::u32::MIN);
        Ok(())
    }

    #[test]
    fn rw_u64() -> Result<()> {
        let mut b = Buffer::new(std::mem::size_of::<u64>() * 3);
        b.write_u64(std::u64::MAX - 1)?;
        b.write_u64(0u64)?;
        b.write_u64(std::u64::MIN)?;
        let err = b.write_u64(2);
        assert!(err.is_err());
        assert_eq!(b.read_u64(0)?, std::u64::MAX - 1);
        assert_eq!(b.read_u64(8)?, 0u64);
        assert_eq!(b.read_u64(16)?, std::u64::MIN);
        Ok(())
    }
}
