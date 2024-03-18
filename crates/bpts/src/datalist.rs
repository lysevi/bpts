use bpts_tree::types::Id;

/*
[
{data,key,data_len,,key_len,id}
]
*/

unsafe fn write_slice(buffer: *mut u8, offset: usize, target: &[u8]) -> usize {
    let mut ptr = buffer.add(offset);

    for i in target.iter() {
        ptr.write(*i);
        ptr = ptr.add(std::mem::size_of::<u8>()); // :-)
    }
    return offset + target.len();
}

pub fn get_pack_size(id: Id, key: &[u8], data: &[u8]) -> usize {
    let mut pack_size = std::mem::size_of_val(&id.0);
    pack_size += std::mem::size_of::<u32>() * 2;
    pack_size += key.len() + data.len();
    return pack_size;
}

// return offset;
pub unsafe fn insert(buffer: *mut u8, offset: u32, id: Id, key: &[u8], data: &[u8]) -> Option<u32> {
    let mut write_pos = offset as usize;
    let result = write_pos as u32;

    let ptr = buffer.add(write_pos) as *mut u32;
    ptr.write(id.0);
    write_pos = write_pos + std::mem::size_of::<u32>();

    let ptr = buffer.add(write_pos) as *mut u32;
    ptr.write(key.len() as u32);
    write_pos = write_pos + std::mem::size_of::<u32>();

    let ptr = buffer.add(write_pos) as *mut u32;
    ptr.write(data.len() as u32);
    write_pos = write_pos + std::mem::size_of::<u32>();

    write_pos = write_slice(buffer, write_pos, key);
    write_slice(buffer, write_pos, data);
    Some(result)
}

pub unsafe fn load<'a>(buffer: *const u8, offset: u32) -> (Id, &'a [u8], &'a [u8]) {
    let mut read_offset = offset as usize;

    let id = (buffer.add(read_offset) as *const u32).read();
    read_offset += std::mem::size_of::<u32>();

    let key_len = (buffer.add(read_offset) as *const u32).read();
    read_offset += std::mem::size_of::<u32>();

    let data_len = (buffer.add(read_offset) as *const u32).read();
    read_offset += std::mem::size_of::<u32>();

    let key = std::slice::from_raw_parts(buffer.add(read_offset), key_len as usize);
    read_offset += key_len as usize;
    let data = std::slice::from_raw_parts(buffer.add(read_offset), data_len as usize);

    return (Id(id), key, data);
}

pub unsafe fn load_key<'a>(buffer: *const u8, offset: u32) -> (Id, &'a [u8]) {
    let mut read_offset = offset as usize;

    let id = (buffer.add(read_offset) as *const u32).read();
    read_offset += std::mem::size_of::<u32>();

    let key_len = (buffer.add(read_offset) as *const u32).read();
    read_offset += std::mem::size_of::<u32>();
    read_offset += std::mem::size_of::<u32>();

    let key = std::slice::from_raw_parts(buffer.add(read_offset), key_len as usize);

    return (Id(id), key);
}

#[cfg(test)]
mod tests {
    use bpts_tree::prelude::*;
    #[test]
    fn save() -> Result<()> {
        const BUFFERSIZE: usize = 100;
        let mut buffer = vec![0u8; BUFFERSIZE];

        let offset1 = unsafe {
            super::insert(buffer.as_mut_ptr(), 0, Id(1), &[1, 2, 3], &[3, 2, 1]).unwrap()
        };
        let offset2 = unsafe {
            super::insert(
                buffer.as_mut_ptr(),
                offset1 + super::get_pack_size(Id(1), &[1, 2, 3], &[3, 2, 1]) as u32,
                Id(2),
                &[4, 5, 6],
                &[7, 8, 9],
            )
            .unwrap()
        };

        let (id1, key1, data1) = unsafe { super::load(buffer.as_ptr(), offset1) };
        let (id2, key2, data2) = unsafe { super::load(buffer.as_ptr(), offset2) };

        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);

        assert_eq!(key1, [1, 2, 3]);
        assert_eq!(key2, [4, 5, 6]);

        assert_eq!(data1, [3, 2, 1]);
        assert_eq!(data2, [7, 8, 9]);

        let (id1, key1) = unsafe { super::load_key(buffer.as_ptr(), offset1) };
        let (id2, key2) = unsafe { super::load_key(buffer.as_ptr(), offset2) };

        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);

        assert_eq!(key1, [1, 2, 3]);
        assert_eq!(key2, [4, 5, 6]);
        Ok(())
    }
}
