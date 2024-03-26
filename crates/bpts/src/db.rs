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
