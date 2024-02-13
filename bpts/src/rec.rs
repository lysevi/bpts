use crate::types;
use crate::utils;

use types::Id;

#[derive(Debug, Clone)]
pub struct Record {
    value: Vec<u8>,
}

impl Record {
    pub fn from_u8(value: u8) -> Self {
        let mut v = Vec::new();
        v.push(value);
        Record { value: v }
    }

    pub fn from_i32(value: i32) -> Self {
        let slice = unsafe { utils::any_as_u8_slice(&value) };
        Record {
            value: Vec::from(slice),
        }
    }

    pub fn into_u8(&self) -> u8 {
        assert!(!self.value.is_empty());
        return self.value[0];
    }

    pub fn into_i32(&self) -> i32 {
        let mut slice: [u8; 4] = [0; 4];
        for i in 0..4 {
            slice[i] = self.value[i];
        }
        return i32::from_le_bytes(slice);
    }

    pub fn from_id(value: Id) -> Self {
        Record::from_i32(value)
    }

    pub fn into_id(&self) -> Id {
        self.into_i32()
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        return self.value.partial_cmp(&other.value);
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        return self.value == other.value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_test() {
        {
            let d1 = Record::from_u8(1);
            assert_eq!(d1.value[0], 1);

            let value: u8 = d1.into_u8();
            assert_eq!(value, 1);
        }

        {
            let d1 = Record::from_i32(1i32);
            let value: i32 = d1.into_i32();
            assert_eq!(value, 1i32);
        }
    }
}
