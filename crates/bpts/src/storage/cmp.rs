use std::{cell::RefCell, rc::Rc};

use super::{flat_storage::FlatStorage, store::Storage, KeyCmp};
use crate::tree::node::NodeKeyCmp;

pub struct StorageNodeCmp {
    pub(super) store: Rc<RefCell<dyn FlatStorage>>,
    pub(super) cmp: Rc<RefCell<dyn KeyCmp>>,
}

impl NodeKeyCmp for StorageNodeCmp {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        let store = self.store.borrow();
        let kv1 = Storage::read_kv(&*store, key1 as usize).unwrap();
        let kv2 = Storage::read_kv(&*store, key2 as usize).unwrap();
        return self.cmp.borrow().compare(&kv1.0, &kv2.0);
    }
}

pub(super) struct StorageKeyCmpRef {
    pub(super) user_key: Vec<u8>,
    pub(super) store: Rc<RefCell<dyn FlatStorage>>,
    pub(super) cmp: Rc<RefCell<dyn KeyCmp>>,
}

impl StorageKeyCmpRef {
    fn cmp_with_left(&self, key2: u32) -> std::cmp::Ordering {
        let store = self.store.borrow();
        let kv2 = Storage::read_kv(&*store, key2 as usize).unwrap();
        return self
            .cmp
            .borrow()
            .compare(self.user_key.as_slice(), kv2.0.as_slice());
    }

    fn cmp_with_right(&self, key1: u32) -> std::cmp::Ordering {
        let store = self.store.borrow();
        let kv1 = Storage::read_kv(&*store, key1 as usize).unwrap();
        return self
            .cmp
            .borrow()
            .compare(kv1.0.as_slice(), self.user_key.as_slice());
    }
}

impl NodeKeyCmp for StorageKeyCmpRef {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        if key1 == std::u32::MAX && key2 == std::u32::MAX {
            return std::cmp::Ordering::Equal;
        }

        if key1 != std::u32::MAX && key2 != std::u32::MAX {
            let store = self.store.borrow();
            let kv1 = Storage::read_kv(&*store, key1 as usize).unwrap();
            let kv2 = Storage::read_kv(&*store, key2 as usize).unwrap();
            return self.cmp.borrow().compare(&kv1.0, &kv2.0);
        }

        if key1 == std::u32::MAX && key2 != std::u32::MAX {
            return self.cmp_with_left(key2);
        }

        return self.cmp_with_right(key1);
    }
}
