use std::iter;

pub(crate) fn jenkins_hash(key: &[u8]) -> u32 {
    let mut hash = 0u32;

    for item in key.iter() {
        hash += *item as u32;
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

pub struct Bloom {
    //TODO use: * u8
    data: Vec<u8>,
}

impl Bloom {
    pub fn new(buflen: usize) -> Bloom {
        Bloom {
            data: iter::repeat(0).take(buflen).collect(),
        }
    }

    pub fn from_array(a: &[u8]) -> Bloom {
        Bloom { data: a.to_vec() }
    }

    pub fn as_array(&self) -> &[u8] {
        &self.data
    }

    pub fn bit_count(&self) -> usize {
        self.data.len() * 8
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn insert(&mut self, key: &[u8]) {
        let _jh = jenkins_hash(key);

        todo!()
    }

    pub fn check(&self, _key: &[u8]) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{bloom::jenkins_hash, utils::any_as_u8_slice};

    #[test]
    fn jenkins() {
        let h1 = jenkins_hash(&[1, 2, 3]);
        let h2 = jenkins_hash(&[1, 2, 3]);
        let h3 = jenkins_hash(&[1, 2, 2]);
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    #[ignore]
    fn filter() {
        let mut bl = super::Bloom::new(16);
        assert_eq!(bl.size(), 16);
        assert_eq!(bl.bit_count(), 16 * 8);

        for i in 1..10 {
            let key = unsafe { any_as_u8_slice(&i) };
            assert!(!bl.check(key));
            bl.insert(key);
            assert!(bl.check(key));
        }
    }
}
