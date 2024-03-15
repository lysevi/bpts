use bpts_tree::types::Id;

/*
[
free space ... {data,key,data_len,,key_len,id}, {data,key,data_len,,key_len,id}, {data,key,data_len,,key_len,id}
]
*/

#[repr(C, packed)]
pub struct DataList {
    buffer: *mut u8,
    cap: u32,
    offset_from_end: u32,
}

impl DataList {
    pub fn new(buffer: *mut u8, cap: u32, offset: u32) -> DataList {
        DataList {
            buffer,
            cap,
            offset_from_end: offset,
        }
    }

    pub fn get_offset(&self) -> u32 {
        self.offset_from_end
    }

    unsafe fn wrte_slice(&self, offset: usize, target: &[u8]) -> usize {
        let mut ptr = self.buffer.add(offset).sub(target.len());

        for i in target.iter() {
            ptr.write(*i);
            ptr = ptr.add(std::mem::size_of::<u8>()); // :-)
        }
        return offset - target.len();
    }

    // return offset;
    pub unsafe fn insert(&mut self, id: Id, key: &[u8], data: &[u8]) -> Option<u32> {
        let pack_size =
            std::mem::size_of_val(&id.0) + std::mem::size_of::<u32>() * 2 + key.len() + data.len();
        if pack_size > (self.cap - self.offset_from_end) as usize {
            return None;
        }
        let result = self.offset_from_end;
        let mut write_pos =
            (self.cap - self.offset_from_end) as usize - std::mem::size_of_val(&id.0);

        let ptr = self.buffer.add(write_pos) as *mut u32;
        ptr.write(id.0);

        write_pos = write_pos - std::mem::size_of::<u32>();
        let ptr = self.buffer.add(write_pos) as *mut u32;
        ptr.write(key.len() as u32);

        write_pos = write_pos - std::mem::size_of::<u32>();
        let ptr = self.buffer.add(write_pos) as *mut u32;
        ptr.write(data.len() as u32);

        write_pos = self.wrte_slice(write_pos, key);
        write_pos = self.wrte_slice(write_pos, data);

        self.offset_from_end = self.cap - write_pos as u32;
        Some(result)
    }

    pub unsafe fn load(&self, offset: u32) -> (Id, &[u8], &[u8]) {
        let mut read_offset = (self.cap - offset) as usize;

        read_offset -= std::mem::size_of::<u32>();
        let id = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= std::mem::size_of::<u32>();
        let key_len = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= std::mem::size_of::<u32>();
        let data_len = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= key_len as usize;
        let key = std::slice::from_raw_parts(self.buffer.add(read_offset), key_len as usize);
        read_offset -= data_len as usize;
        let data = std::slice::from_raw_parts(self.buffer.add(read_offset), data_len as usize);

        return (Id(id), key, data);
    }

    pub unsafe fn load_key(&self, offset: u32) -> (Id, &[u8]) {
        let mut read_offset = (self.cap - offset) as usize;

        read_offset -= std::mem::size_of::<u32>();
        let id = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= std::mem::size_of::<u32>();
        let key_len = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= std::mem::size_of::<u32>();
        //let data_len = (self.buffer.add(read_offset) as *const u32).read();

        read_offset -= key_len as usize;
        let key = std::slice::from_raw_parts(self.buffer.add(read_offset), key_len as usize);

        return (Id(id), key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bpts_tree::prelude::*;

    use super::DataList;
    #[test]
    fn save() -> Result<()> {
        const BUFFERSIZE: usize = 100;
        let mut buffer = vec![0u8; BUFFERSIZE + 10];
        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            buffer[pos] = i as u8;
        }
        let mut dl = DataList::new(buffer.as_mut_ptr(), BUFFERSIZE as u32, 0u32);
        let offset = dl.get_offset();
        assert_eq!(offset, 0);

        let offset1 = unsafe { dl.insert(Id(1), &[1, 2, 3], &[3, 2, 1]).unwrap() };
        assert!(offset < dl.get_offset());
        let offset = dl.get_offset();

        let offset2 = unsafe { dl.insert(Id(2), &[4, 5, 6], &[7, 8, 9]).unwrap() };
        assert!(offset < dl.get_offset());

        for i in 0..10 {
            let pos = buffer.len() - 1 - i;
            assert_eq!(buffer[pos], i as u8);
        }

        let (id1, key1, data1) = unsafe { dl.load(offset1) };
        let (id2, key2, data2) = unsafe { dl.load(offset2) };

        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);

        assert_eq!(key1, [1, 2, 3]);
        assert_eq!(key2, [4, 5, 6]);

        assert_eq!(data1, [3, 2, 1]);
        assert_eq!(data2, [7, 8, 9]);

        let (id1, key1) = unsafe { dl.load_key(offset1) };
        let (id2, key2) = unsafe { dl.load_key(offset2) };

        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);

        assert_eq!(key1, [1, 2, 3]);
        assert_eq!(key2, [4, 5, 6]);
        Ok(())
    }

    #[test]
    fn save_small_buffer() -> Result<()> {
        const BUFFERSIZE: usize = 5;
        let mut buffer = vec![0u8; BUFFERSIZE];

        let mut dl = DataList::new(buffer.as_mut_ptr(), BUFFERSIZE as u32, 0u32);

        let offset = unsafe { dl.insert(Id(1), &[1, 2, 3], &[3, 2, 1]) };
        assert!(offset.is_none());

        Ok(())
    }

    #[test]
    fn many_inserts() -> Result<()> {
        const BUFFERSIZE: usize = 1024;
        let mut buffer = vec![0u8; BUFFERSIZE];

        let mut dl = DataList::new(buffer.as_mut_ptr(), BUFFERSIZE as u32, 0u32);
        let offset = dl.get_offset();
        assert_eq!(offset, 0);

        let mut values = HashMap::new();
        let mut id = 1;
        loop {
            let offset = unsafe { dl.insert(Id(id), &[id as u8, 1], &[id as u8, id as u8, 1]) };
            if offset.is_none() {
                break;
            }
            values.insert(id, offset.unwrap());
            id += 1;
        }

        assert!(values.len() > 10);

        for i in values {
            let etalon_key = [i.0 as u8, 1];
            let (id, key, data) = unsafe { dl.load(i.1) };

            assert_eq!(id.0, i.0);
            assert_eq!(key, etalon_key);
            assert_eq!(data, [i.0 as u8, i.0 as u8, 1]);

            let (id, key) = unsafe { dl.load_key(i.1) };
            assert_eq!(id.0, i.0);
            assert_eq!(key, etalon_key);
        }

        Ok(())
    }
}
