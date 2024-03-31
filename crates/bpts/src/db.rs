/*
DbHeader:DataBlock;Page1;Page2...PageN:DataBlock
*/

pub struct DbHeader {
    is_closed_normalu: bool,
}

pub struct DataBlockHeader {
    pub freelist_size: u32,
    pub next_data_block_offset: u32,
}

pub trait PageStorage {}

pub struct Db<PS: PageStorage> {
    pstore: Box<PS>,
}

impl<PS: PageStorage> Db<PS> {
    pub fn new(pstore: Box<PS>) -> Db<PS> {
        Db { pstore }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;

    struct MockPageStorage {}

    impl PageStorage for MockPageStorage {}

    #[test]
    fn db() -> Result<()> {
        let mut db = Db::new(Box::new(MockPageStorage {}));
        Ok(())
    }
}
