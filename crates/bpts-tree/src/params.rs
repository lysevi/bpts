#[derive(Clone, Copy)]
pub struct TreeParams {
    pub t: usize,
    pub min_size_root: usize,
    pub min_size_node: usize,
    pub min_size_leaf: usize,
}

impl TreeParams {
    pub fn default() -> TreeParams {
        TreeParams {
            t: 100,
            min_size_leaf: 50,
            min_size_root: 2,
            min_size_node: 50,
        }
    }

    pub fn default_with_t(t: usize) -> TreeParams {
        TreeParams {
            t: t,
            min_size_leaf: t,
            min_size_root: t,
            min_size_node: t,
        }
    }

    pub fn with_t(mut self, t: usize) -> TreeParams {
        self.t = t;
        return self;
    }

    pub fn with_min_size_leaf(mut self, v: usize) -> Self {
        self.min_size_leaf = v;
        self
    }

    pub fn with_min_size_root(mut self, v: usize) -> Self {
        self.min_size_root = v;
        self
    }

    pub fn with_min_size_node(mut self, v: usize) -> Self {
        self.min_size_node = v;
        self
    }

    pub fn get_min_size_leaf(&self) -> usize {
        self.min_size_leaf
    }

    pub fn get_min_size_root(&self) -> usize {
        self.min_size_root
    }

    pub fn get_min_size_node(&self) -> usize {
        self.min_size_node
    }

    pub fn get_t(&self) -> usize {
        self.t
    }
}
