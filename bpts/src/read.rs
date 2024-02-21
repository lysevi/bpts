use std::rc::Rc;

use crate::node::*;
use crate::nodestorage::NodeStorage;
use crate::rec::Record;
use crate::types::{self, EMPTY_ID};

pub fn scan<'a>(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
) -> Result<RcNode, types::Error> {
    let mut target = Rc::clone(root);

    loop {
        let node_id: types::Id;
        {
            let ref_target = target.borrow();
            if ref_target.is_leaf {
                return Ok(Rc::clone(&target));
            }
            let rec = ref_target.find(key);
            if rec.is_none() {
                return Err(types::Error(format!("{} not found", key)));
            }
            node_id = rec.unwrap().into_id();
        }
        let tmp = storage.get_node(node_id);
        match tmp {
            Ok(r) => {
                target = Rc::clone(&r);
            }
            Err(e) => {
                return Err(types::Error(format!("{:?} not found - '{}'", node_id, e)));
            }
        }
    }
}

pub fn find<'a>(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    key: i32,
) -> Result<Record, types::Error> {
    let node = scan(storage, root, key);
    match node {
        Ok(n) => {
            let b = n.borrow();
            let r = b.find(key);
            return Ok(r.unwrap().clone());
        }
        Err(e) => Err(e),
    }
}

pub fn map<'a, F>(
    storage: &mut dyn NodeStorage,
    root: &RcNode,
    from: i32,
    to: i32,
    f: &mut F,
) -> Result<(), types::Error>
where
    F: FnMut(i32, &Record),
{
    let node_from = scan(storage, root, from);
    let node_to = scan(storage, root, to);

    match node_from {
        Ok(_) => {}
        Err(e) => return Err(e),
    }

    match node_to {
        Ok(_) => {}
        Err(e) => return Err(e),
    }
    let from_unwr = node_from.unwrap();
    let to_unwr = node_to.unwrap();

    let mut cur_node: RcNode = from_unwr.clone();

    loop {
        {
            let ref_to = cur_node.borrow();
            ref_to.map(from, to, f);
            if ref_to.id == to_unwr.borrow().id {
                break;
            }
        }
        println!(
            "map: {:?} {:?}",
            cur_node.borrow().id,
            cur_node.borrow().right
        );
        if cur_node.borrow().right != EMPTY_ID {
            let next = storage.get_node(cur_node.borrow().right);

            cur_node = next.unwrap();
        } else {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mocks::MockNodeStorage, node::Node};

    #[test]
    fn find_in_tree() {
        let leaf1 = Node::new_leaf(
            types::Id(0),
            vec![2, 3],
            vec![Record::from_u8(2), Record::from_u8(3)],
            2,
            2,
        );

        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf1);
        let res = find(&mut storage, &leaf1, 2);
        assert!(res.is_ok());
        assert_eq!(res.unwrap().into_u8(), 2u8);

        let leaf2 = Node::new_leaf(types::Id(1), vec![1], vec![Record::from_u8(1)], 1, 1);
        storage.add_node(&leaf2);

        let node1 = Node::new_root(
            types::Id(2),
            vec![2],
            vec![Record::from_id(types::Id(1)), Record::from_id(types::Id(0))],
            1,
            2,
        );

        storage.add_node(&node1);
        let res_1 = find(&mut storage, &node1, 1);
        assert!(res_1.is_ok());
        assert_eq!(res_1.unwrap().into_u8(), 1u8);

        let res_2 = find(&mut storage, &node1, 2);
        assert!(res_2.is_ok());
        assert_eq!(res_2.unwrap().into_u8(), 2u8);
    }
    #[test]
    #[ignore]
    fn bulk_read() {
        todo!()
    }
}
