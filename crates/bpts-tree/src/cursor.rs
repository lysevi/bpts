use crate::{
    node::{Node, RcNode},
    nodestorage::{self, NodeStorage},
    prelude::Record,
};

pub enum CursorState {
    End,
    Work,
}

impl CursorState {
    pub fn is_end(self) -> bool {
        match self {
            CursorState::End => true,
            CursorState::Work => false,
        }
    }
}

pub enum CursorDirection {
    Forward,
    Backward,
}

pub struct Cursor<'a, Storage: nodestorage::NodeStorage> {
    storage: &'a mut Storage,
    begin: RcNode,
    end: RcNode,
    from: u32,
    to: u32,
    dir: CursorDirection,
}

impl<'a, Storage: NodeStorage> Cursor<'a, Storage> {
    pub fn new(
        s: &'a mut Storage,
        begin: RcNode,
        end: RcNode,
        dir: CursorDirection,
        from: u32,
        to: u32,
    ) -> Cursor<Storage> {
        Cursor {
            storage: s,
            begin,
            end,
            dir,
            from,
            to,
        }
    }

    fn step_fwd<F>(&mut self, node: &Node, f: &mut F) -> crate::Result<CursorState>
    where
        F: FnMut(u32, &Record),
    {
        node.map(self.from, self.to, f);
        if self.begin.borrow().id == self.end.borrow().id {
            return Ok(CursorState::End);
        }

        if self.begin.borrow().right.exists() {
            let next = self.storage.get_node(self.begin.borrow().right);
            self.begin = next.unwrap();
            return Ok(CursorState::Work);
        } else {
            return Ok(CursorState::End);
        }
    }

    fn step_bwd<F>(&mut self, node: &Node, f: &mut F) -> crate::Result<CursorState>
    where
        F: FnMut(u32, &Record),
    {
        node.map_rev(self.from, self.to, f);

        if node.id == self.begin.borrow().id {
            return Ok(CursorState::End);
        }

        if node.left.exists() {
            let next = self.storage.get_node(node.left);
            self.end = next.unwrap();
            return Ok(CursorState::Work);
        } else {
            return Ok(CursorState::End);
        }
    }

    pub fn next<F>(&mut self, f: &mut F) -> crate::Result<CursorState>
    where
        F: FnMut(u32, &Record),
    {
        match self.dir {
            CursorDirection::Forward => {
                let n = self.begin.clone();
                return self.step_fwd(&n.borrow(), f);
            }
            CursorDirection::Backward => {
                let n = self.end.clone();
                return self.step_bwd(&n.borrow(), f);
            }
        }
    }
}
