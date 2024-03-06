use std::fmt::Display;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Id(pub i32);

pub struct Ptr(u32);
#[derive(Debug)]
pub struct Error(pub String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error({})", self.0)
    }
}

const EMPTY_ID: Id = Id(-1);

impl Id {
    pub fn empty() -> Id {
        EMPTY_ID
    }
    pub fn unwrap(&self) -> i32 {
        self.0
    }

    pub fn is_empty(self) -> bool {
        return self == EMPTY_ID;
    }

    pub fn exists(self) -> bool {
        return !self.is_empty();
    }

    pub fn clear(&mut self) {
        self.0 = EMPTY_ID.0;
    }
}
