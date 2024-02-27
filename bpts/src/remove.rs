use crate::{node::RcNode, nodestorage::NodeStorage, types, utils};

pub fn erase_key(
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
            if target_node_ref.left != types::EMPTY_ID {
                //TODO! check result;
                let low_side_leaf = storage.get_node(target_node_ref.left).unwrap();
                let mut low_side_leaf_ref = low_side_leaf.borrow_mut();
                if low_side_leaf_ref.data_count > t {
                    let max_key = low_side_leaf_ref.keys[low_side_leaf_ref.keys_count - 1];
                    let max_data = low_side_leaf_ref.data[low_side_leaf_ref.data_count - 1].clone();

                    utils::insert_to_array(&mut target_node_ref.keys, 0, max_key);
                    utils::insert_to_array(&mut target_node_ref.data, 0, max_data);
                    low_side_leaf_ref.keys_count -= 1;
                    low_side_leaf_ref.data_count -= 1;

                    target_node_ref.keys_count += 1;
                    target_node_ref.data_count += 1;
                }
            }
            return Ok(toproot.unwrap());
        } else {
            return Ok(toproot.unwrap());
        }
    } else {
        todo!();
    }
}

#[cfg(test)]
mod tests {
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

        let result = erase_key(&mut storage, &leaf, 2, 3, Some(leaf.clone()));
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

    #[test]
    fn remove_from_leaf_take_from_lower() {
        let leaf_high = Node::new_leaf(
            types::Id(1),
            vec![5, 6, 7, 0],
            vec![
                Record::from_u8(5),
                Record::from_u8(6),
                Record::from_u8(7),
                Record::from_u8(0),
            ],
            3,
            3,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_high);

        let leaf_low = Node::new_leaf(
            types::Id(2),
            vec![1, 2, 3, 4],
            vec![
                Record::from_u8(1),
                Record::from_u8(2),
                Record::from_u8(3),
                Record::from_u8(4),
            ],
            4,
            4,
        );
        let mut storage: MockNodeStorage = MockNodeStorage::new();
        storage.add_node(&leaf_low);
        leaf_high.borrow_mut().left = leaf_low.borrow().id;

        let result = erase_key(&mut storage, &leaf_high, 6, 3, Some(leaf_high.clone()));
        assert!(result.is_ok());

        {
            let root = result.unwrap();
            let ref_root = root.borrow_mut();
            assert_eq!(ref_root.keys, vec![4, 5, 7, 0]);
            assert_eq!(
                ref_root.data,
                vec![
                    Record::from_u8(4),
                    Record::from_u8(5),
                    Record::from_u8(7),
                    Record::from_u8(0),
                ]
            );
            assert_eq!(ref_root.keys_count, 3);
            assert_eq!(ref_root.data_count, 3);
        }
    }
}
