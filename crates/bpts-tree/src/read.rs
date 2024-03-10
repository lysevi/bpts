use std::rc::Rc;

use crate::cursor;
use crate::node::*;
use crate::nodestorage::NodeStorage;
use crate::rec::Record;
use crate::types::{self};
use crate::Result;

pub fn scan<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: i32,
) -> Result<RcNode> {
    let mut target = Rc::clone(root);

    loop {
        let node_id: types::Id;
        {
            let ref_target = target.borrow();
            //println!("scan > {:?}", ref_target.id);
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

pub fn find<Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    key: i32,
) -> Result<Option<Record>> {
    let node = scan(storage, root, key)?;
    let r = node.borrow();
    return Ok(r.find(key));
}

pub fn map<F, Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    from: i32,
    to: i32,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(i32, &Record),
{
    assert!(from <= to);
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
    let mut crs = cursor::Cursor::new(
        storage,
        node_from.unwrap(),
        node_to.unwrap(),
        cursor::CursorDirection::Forward,
        from,
        to,
    );

    while !crs.next(f)?.is_end() {}

    Ok(())
}

pub fn map_rev<F, Storage: NodeStorage>(
    storage: &mut Storage,
    root: &RcNode,
    from: i32,
    to: i32,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(i32, &Record),
{
    assert!(from <= to);
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
    let mut crs = cursor::Cursor::new(
        storage,
        node_from.unwrap(),
        node_to.unwrap(),
        cursor::CursorDirection::Backward,
        from,
        to,
    );

    while !crs.next(f)?.is_end() {}

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mocks::MockNodeStorage, node::Node};

    #[test]
    fn find_in_tree() -> Result<()> {
        let leaf1 = Node::new_leaf(
            types::Id(0),
            vec![2, 3],
            vec![Record::from_i32(2), Record::from_i32(3)],
            2,
            2,
        );

        let mut storage: MockNodeStorage =
            MockNodeStorage::new(crate::params::TreeParams::default_with_t(3));
        storage.add_node(&leaf1);
        let res = find(&mut storage, &leaf1, 2)?;
        assert!(res.is_some());
        assert_eq!(res.unwrap().into_i32(), 2);

        let leaf2 = Node::new_leaf(types::Id(1), vec![1], vec![Record::from_i32(1)], 1, 1);
        storage.add_node(&leaf2);

        let node1 = Node::new_root(
            types::Id(2),
            vec![2],
            vec![Record::from_id(types::Id(1)), Record::from_id(types::Id(0))],
            1,
            2,
        );

        storage.add_node(&node1);
        let res_1 = find(&mut storage, &node1, 1)?;
        assert!(res_1.is_some());
        assert_eq!(res_1.unwrap().into_i32(), 1);

        let res_2 = find(&mut storage, &node1, 2)?;
        assert!(res_2.is_some());
        assert_eq!(res_2.unwrap().into_i32(), 2);
        return Ok(());
    }
}
