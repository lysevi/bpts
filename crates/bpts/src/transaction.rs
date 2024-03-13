use std::collections::HashMap;

use bpts_tree::prelude::*;

#[repr(C, packed)]
#[derive(Debug, Clone)]
pub struct TransactionHeader {
    rev: u32,
    tree_id: u32,
    size: u32,
}
const TRANSACTION_HEADER_SIZE: u32 = std::mem::size_of::<TransactionHeader>() as u32;

#[derive(Clone)]
pub struct Transaction {
    header: TransactionHeader,
    buffer: Option<*mut u8>,
    offset: u32,
    nodes: HashMap<i32, RcNode>,
    params: TreeParams,
}

impl Transaction {
    pub fn new(rev: u32, tree_id: u32, params: TreeParams) -> Transaction {
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
        }
    }

    pub unsafe fn from_buffer(buffer: *mut u8, offset: u32, params: TreeParams) -> Transaction {
        let ptr = buffer as *const TransactionHeader;
        let hdr = std::ptr::read(ptr);
        Transaction {
            header: hdr,
            buffer: Some(buffer),
            offset: offset,
            nodes: HashMap::new(),
            params: params,
        }
    }

    pub unsafe fn save_to(&mut self, buffer: *mut u8, offset: u32) -> u32 {
        let src_ptr = &self.header as *const TransactionHeader;
        let dest_ptr = buffer as *mut TransactionHeader;
        std::ptr::copy(src_ptr, dest_ptr, 1);
        self.offset = offset;
        self.buffer = Some(buffer);
        return self.size();
    }

    pub fn from_transaction(other: &Transaction) -> Transaction {
        todo!();
        // Transaction {
        //     header: other.header.clone(),
        //     buffer: None,
        //     offset: 0,
        //     nodes: HashMap::new(),
        //     params: other.params,
        // }
    }

    pub fn size(&self) -> u32 {
        return TRANSACTION_HEADER_SIZE;
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

impl NodeStorage for Transaction {
    fn get_root(&self) -> Option<RcNode> {
        for i in &self.nodes {
            let node = i.1;
            if !node.borrow().is_leaf && node.borrow().parent.is_empty() {
                return Some(node.clone());
            }
        }
        None
    }
    fn get_new_id(&self) -> Id {
        if !self.is_readonly() {
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

    fn get_node(&self, id: Id) -> Result<RcNode> {
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(r.clone())
        } else {
            Err(bpts_tree::types::Error(format!("not found Id={}", id.0)))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        if !self.is_readonly() {
            panic!("logic error");
        }
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        if !self.is_readonly() {
            panic!("logic error");
        }
        println!("erase node: Id={}", id.0);
        self.nodes.remove(&id.0);
    }

    fn get_params(&self) -> &TreeParams {
        &self.params
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::Result;

    #[test]
    fn transaction_storage() -> Result<()> {
        todo!();
        Ok(())
    }
}
