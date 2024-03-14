pub mod datalist;
pub mod mfile;
pub mod page;
pub mod prelude;
pub mod transaction;
pub mod utils;
pub type Result<T> = bpts_tree::Result<T>;

pub struct Storage {
    pages: Vec<page::Page>,
}

impl Storage {
    pub fn new() -> Storage {
        Storage { pages: Vec::new() }
    }

    pub fn pages_count(&self) -> usize {
        self.pages.len()
    }
}
