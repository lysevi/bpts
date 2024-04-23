use std::fmt::Display;
pub mod bloom;
pub mod mfile;
pub mod prelude;
pub mod storage;
pub mod tree;
pub mod types;
pub mod utils;

#[derive(Debug)]
pub enum Error {
    Fail(String),
    IsFull,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error({:?})", self)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
//#[cfg(feature = "verbose")]
macro_rules! verbose {
    () => {
        print!("\n")
    };
    ($($arg:tt)*) => {{
        println!($($arg)*)
    }};
}

// #[macro_export]
// #[cfg(not(feature = "verbose"))]
// macro_rules! verbose {
//     () => {};
//     ($($arg:tt)*) => {{}};
// }
