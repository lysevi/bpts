use crate::{node::RcNode, nodestorage::NodeStorage, types, utils};

pub fn remove(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    key: i32,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    if target_node.borrow().is_leaf {
        let mut target_node_ref = target_node.borrow_mut();
        for i in 0..target_node_ref.keys_count {
            if target_node_ref.keys[i] == key {
                utils::remove_with_shift(&mut target_node_ref.keys, i);
                utils::remove_with_shift(&mut target_node_ref.data, i);
                target_node_ref.keys_count -= 1;
                target_node_ref.data_count -= 1;
                break;
            }
        }
        if target_node_ref.data_count < t {
            todo!();
        } else {
            return Ok(toproot.unwrap());
        }
    } else {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::node::Node;
    use crate::rec::Record;

    #[test]
    fn remove_from_leaf() {
        let leaf = Node::new_leaf(
            types::Id(1),
            vec![1, 2, 3, 4, 5, 6],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
                Record::from_u8(5),
                Record::from_u8(6),
            ],
            6,
            6,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf);

        let result = remove(&mut storage, &leaf, 2, 3, Some(leaf.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![1, 3, 4, 5, 6, 2]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_u8(1),
                    Record::from_u8(3),
                    Record::from_u8(4),
                    Record::from_u8(5),
                    Record::from_u8(6),
                    Record::from_u8(2),
                ]
            );
            assert_eq!(ref_root.keys_count, 5);
            assert_eq!(ref_root.data_count, 5);
        }
    }
}
