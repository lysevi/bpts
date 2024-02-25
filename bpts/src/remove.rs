use crate::{node::RcNode, nodestorage::NodeStorage, types};

pub fn remove(
    storage: &mut dyn NodeStorage,
    target_node: &RcNode,
    t: usize,
    toproot: Option<RcNode>,
) -> Result<RcNode, types::Error> {
    todo!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::MockNodeStorage;
    use crate::node::Node;
    use crate::read::{self, find};
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
        let result = remove(&mut storage, &leaf, 3, Some(leaf.clone()));
        assert!(result.is_ok())
    }
}
