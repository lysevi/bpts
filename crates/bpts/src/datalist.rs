use bpts_tree::types::Id;

#[repr(C, packed)]
pub struct DataList {
    buffer: *mut u8,
    cap: usize,
}

impl DataList {
    pub fn new(buffer: *mut u8, cap: usize) -> DataList {
        DataList { buffer, cap }
    }

    // return offset;
    pub fn insert(&mut self, id: Id, key: &[u8], data: &[u8]) -> u32 {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use bpts_tree::prelude::*;

    use super::DataList;
    #[test]
    fn datalist_save() -> Result<()> {
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut dl = DataList::new(buffer.as_mut_ptr(), buffer.len());

        let offset1 = dl.insert(Id(1), &[1, 2, 3], &[3, 2, 1]);

        let offset2 = dl.insert(Id(2), &[4, 5, 6], &[7, 8, 9]);

        Ok(())
    }
}
