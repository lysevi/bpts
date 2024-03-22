#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Id(pub u32);

const EMPTY_ID: Id = Id(std::u32::MAX);

impl Id {
    pub fn empty() -> Id {
        EMPTY_ID
    }
    pub fn unwrap(&self) -> u32 {
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
