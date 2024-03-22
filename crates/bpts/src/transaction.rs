use std::{cell::RefCell, collections::HashMap, rc::Rc};

use crate::{
    tree::{
        node::{KeyCmp, Node, RcNode},
        params::TreeParams,
        record::Record,
    },
    types::Id,
    utils::bufferwriter::{BufferWriter, Counter, UnsafeWriter},
};

use crate::tree::nodestorage::NodeStorage;

/*
header, node_count, node1,...,node_N
*/

#[repr(C, packed)]
#[derive(Debug, Clone)]
pub struct TransactionHeader {
    rev: u32,
    tree_id: u32,
    size: u32,
}

pub type TransKeyCmp = Rc<RefCell<dyn KeyCmp>>;

#[derive(Clone)]
pub struct Transaction {
    header: TransactionHeader,
    buffer: Option<*mut u8>,
    offset: u32,
    nodes: HashMap<u32, RcNode>,
    params: TreeParams,
    cmp: TransKeyCmp,
}

impl Transaction {
    pub fn new(rev: u32, tree_id: u32, params: TreeParams, keycmp: TransKeyCmp) -> Transaction {
        let hdr = TransactionHeader {
            rev,
            tree_id,
            size: 0,
        };
        Transaction {
            header: hdr,
            buffer: None,
            offset: 0u32,
            nodes: HashMap::new(),
            params: params,
            cmp: keycmp,
        }
    }

    pub unsafe fn from_buffer(
        buffer: *mut u8,
        global_offset: u32,
        keycmp: TransKeyCmp,
        params: TreeParams,
    ) -> Transaction {
        let mut ptr_offset = 0;

        let ptr = buffer as *const TransactionHeader;
        let hdr = std::ptr::read(ptr);
        ptr_offset += std::mem::size_of::<TransactionHeader>();

        let nodes_len: u32 = (buffer.add(ptr_offset) as *const u32).read();
        ptr_offset += std::mem::size_of::<u32>();

        let mut nodes = HashMap::with_capacity(nodes_len as usize);

        for _ in 0..nodes_len {
            let node_id = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<i32>();

            let node_is_leaf = (buffer.add(ptr_offset) as *const bool).read();
            ptr_offset += std::mem::size_of::<bool>();

            let node_parent = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<i32>();

            let node_left = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<i32>();

            let node_right = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<i32>();

            let node_keys_count = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<u32>();

            let node_data_count = (buffer.add(ptr_offset) as *const u32).read();
            ptr_offset += std::mem::size_of::<u32>();

            let mut keys = Vec::with_capacity(params.t);
            keys.resize(params.get_keys_count(), 0u32);

            let mut data = Vec::with_capacity(node_data_count as usize);
            data.resize(params.get_keys_count(), Record::Empty);

            for i in 0..node_keys_count {
                let k = (buffer.add(ptr_offset) as *const u32).read();
                ptr_offset += std::mem::size_of::<i32>();
                keys[i as usize] = k;
            }
            for i in 0..node_data_count {
                let value = (buffer.add(ptr_offset) as *const u32).read();
                ptr_offset += std::mem::size_of::<i32>();

                let rec = if !node_is_leaf {
                    Record::Ptr(Id(value))
                } else {
                    Record::Value(value)
                };
                data[i as usize] = rec;
            }
            let node = Node::new(
                Id(node_id),
                node_is_leaf,
                keys,
                data,
                node_keys_count as usize,
                node_data_count as usize,
            );
            node.borrow_mut().parent = Id(node_parent);
            node.borrow_mut().left = Id(node_left);
            node.borrow_mut().right = Id(node_right);
            nodes.insert(node_id, node);
        }

        Transaction {
            header: hdr,
            buffer: Some(buffer),
            offset: global_offset,
            nodes: nodes,
            params: params,
            cmp: keycmp,
        }
    }

    pub unsafe fn save_to(&mut self, buffer: *mut u8, global_offset: u32) -> u32 {
        let mut writer = UnsafeWriter::new(buffer);
        self.send_to_writer(&mut writer);

        self.offset = global_offset;
        self.buffer = Some(buffer);
        return writer.size() as u32;
    }

    pub fn from_transaction(other: &Transaction) -> Transaction {
        let mut res = unsafe {
            Transaction::from_buffer(
                other.buffer.unwrap(),
                other.offset,
                other.cmp.clone(),
                other.params,
            )
        };
        res.offset = 0;
        res.buffer = None;
        return res;
    }

    pub fn set_cmp(&mut self, c: TransKeyCmp) {
        self.cmp = c;
    }

    fn send_to_writer<Writer: BufferWriter>(&self, writer: &mut Writer) {
        writer.write_sized(&self.header);
        writer.write_u32(self.nodes.len() as u32);

        for node in self.nodes.values() {
            let node_ref = node.borrow();
            writer.write_id(node_ref.id);
            writer.write_bool(node_ref.is_leaf);
            writer.write_id(node_ref.parent);
            writer.write_id(node_ref.left);
            writer.write_id(node_ref.right);
            writer.write_u32(node_ref.keys_count as u32);
            writer.write_u32(node_ref.data_count as u32);
            for k in node_ref.key_iter() {
                writer.write_u32(*k);
            }
            for d in node_ref.data_iter() {
                match *d {
                    Record::Value(v) => writer.write_u32(v),
                    Record::Ptr(ptr) => writer.write_id(ptr),
                    Record::Empty => todo!(),
                }
            }
        }
    }

    pub fn clear_cache(&mut self) {
        self.nodes.clear();
    }

    pub fn nodes_count(&self) -> usize {
        return self.nodes.len();
    }

    pub fn size(&self) -> u32 {
        let mut c = Counter::new();
        self.send_to_writer::<Counter>(&mut c);
        return c.size() as u32;
    }

    pub fn rev(&self) -> u32 {
        self.header.rev
    }

    pub fn tree_id(&self) -> u32 {
        self.header.tree_id
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn is_readonly(&self) -> bool {
        return !self.buffer.is_none();
    }
}

impl KeyCmp for Transaction {
    fn compare(&self, key1: u32, key2: u32) -> std::cmp::Ordering {
        let r = self.cmp.borrow();
        return r.compare(key1, key2);
    }
}

impl NodeStorage for Transaction {
    fn get_root(&self) -> Option<RcNode> {
        if self.nodes.len() == 1 {
            let res = self.nodes.iter().next();
            let res = res.unwrap();
            let res = res.1;
            return Some(res.clone());
        }
        for i in &self.nodes {
            let node = i.1;
            if !node.borrow().is_leaf && node.borrow().parent.is_empty() {
                return Some(node.clone());
            }
        }
        None
    }
    fn get_new_id(&self) -> Id {
        if self.is_readonly() {
            panic!("logic error");
        }
        let max = self.nodes.keys().into_iter().max_by(|x, y| x.cmp(y));
        match max {
            Some(x) => {
                let n = x + 1;
                Id(n)
            }
            None => Id(1),
        }
    }

    fn get_node(&self, id: Id) -> crate::Result<RcNode> {
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(r.clone())
        } else {
            Err(crate::Error(format!("not found Id={}", id.0)))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        if self.is_readonly() {
            panic!("logic error");
        }
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        if self.is_readonly() {
            panic!("logic error");
        }
        println!("erase node: Id={}", id.0);
        self.nodes.remove(&id.0);
    }

    fn get_params(&self) -> &TreeParams {
        &self.params
    }

    fn get_cmp(&self) -> &dyn KeyCmp {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use super::Transaction;
    use crate::tree::nodestorage::NodeStorage;
    use crate::{
        tree::{insert, node::Node, params::TreeParams, read::find, record::Record},
        types::Id,
    };

    #[test]
    fn transaction_save() -> crate::Result<()> {
        let max_node_count = 10;
        let params = TreeParams::default();
        let cmp = Rc::new(RefCell::new(crate::tree::mocks::MockKeyCmp::new()));
        let mut storage = Transaction::new(0, 1, params.clone(), cmp.clone());

        let mut root_node = Node::new_leaf_with_size(Id(1), params.t);

        storage.add_node(&root_node);

        let mut allkeys = HashMap::new();

        let mut key: u32 = 1;
        while storage.nodes_count() < max_node_count {
            key += 1;
            let res = insert::insert(&mut storage, &root_node, key, &Record::from_u32(key));
            allkeys.insert(key, false);
            assert!(res.is_ok());
            root_node = res.unwrap();
        }

        for k in allkeys.keys() {
            let result = find(&mut storage, &root_node, *k)?;
            assert!(result.is_some());
            assert_eq!(result.unwrap(), Record::from_u32(*k));
        }

        let size = storage.size();
        let mut buffer = vec![0u8; size as usize + 50];
        let buffer_len = buffer.len();
        for i in (size as usize)..buffer_len {
            buffer[i] = i as u8;
        }
        {
            let slice = buffer.as_mut_slice();
            let writed_bytes = unsafe { storage.save_to(slice.as_mut_ptr(), 0) };

            assert_eq!(size, writed_bytes);
        }
        for i in (size as usize)..buffer_len {
            assert_eq!(buffer[i], i as u8);
        }

        {
            let slice = buffer.as_mut_slice();
            let loaded_trans =
                unsafe { Transaction::from_buffer(slice.as_mut_ptr(), 0, cmp.clone(), params) };

            assert!(loaded_trans.is_readonly());
            assert_eq!(loaded_trans.nodes_count(), storage.nodes_count());

            for k in allkeys.keys() {
                let root = storage.get_root().unwrap();
                let result = find(&mut storage, &root, *k)?;
                assert!(result.is_some());
                assert_eq!(result.unwrap(), Record::from_u32(*k));
            }

            let copy = Transaction::from_transaction(&loaded_trans);
            assert!(!copy.is_readonly());

            for k in allkeys.keys() {
                let root = copy.get_root().unwrap();
                let result = find(&mut storage, &root, *k)?;
                assert!(result.is_some());
                assert_eq!(result.unwrap(), Record::from_u32(*k));
            }
        }
        Ok(())
    }
}
