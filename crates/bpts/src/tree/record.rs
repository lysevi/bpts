use crate::types;

use types::Id;

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum Record {
    Value(u32),
    Ptr(Id),
    Empty,
}

impl Record {
    pub fn empty_array(count: usize) -> Vec<Record> {
        let mut result = Vec::with_capacity(count);

        for _i in 0..count {
            result.push(Record::Empty);
        }
        result
    }

    pub fn from_u32(v: u32) -> Record {
        Record::Value(v)
    }
    pub fn into_u32(&self) -> u32 {
        match self {
            Record::Value(v) => *v,
            Record::Ptr(_) => panic!(),
            Record::Empty => panic!(),
        }
    }

    pub fn from_id(value: Id) -> Self {
        Record::Ptr(value)
    }

    pub fn into_id(&self) -> Id {
        match self {
            Record::Value(_) => panic!(),
            Record::Ptr(v) => *v,
            Record::Empty => panic!(),
        }
    }
}
