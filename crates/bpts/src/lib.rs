use std::fmt::Display;

pub mod datalist;
pub mod freelist;
pub mod mfile;
pub mod page;
pub mod prelude;
pub mod transaction;
pub mod tree;
pub mod types;
pub mod utils;

#[derive(Debug)]
pub struct Error(pub String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error({})", self.0)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
