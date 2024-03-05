#[cfg(test)]
use crate::{
    node::{self, RcNode},
    nodestorage::NodeStorage,
    types::{self, Id},
};
#[cfg(test)]
use std::{collections::HashMap, rc::Rc};

#[cfg(test)]
use string_builder::Builder;

#[cfg(test)]
pub struct MockNodeStorage {
    nodes: HashMap<i32, RcNode>,
}

#[cfg(test)]
impl MockNodeStorage {
    pub fn size(&self) -> usize {
        return self.nodes.len();
    }
    pub fn new() -> MockNodeStorage {
        MockNodeStorage {
            nodes: HashMap::new(),
        }
    }

    pub fn is_exists(&self, id: Id) -> bool {
        self.nodes.contains_key(&id.0)
    }

    pub fn all<F>(&self, f: F) -> bool
    where
        F: FnMut(&RcNode) -> bool,
    {
        self.nodes.values().all(f)
    }

    pub fn print_state(str_before: &String, str_after: &String) {
        print!("digraph G {{");
        print!("{}", str_before);
        print!("{}", str_after);
        println!("}}");
    }

    pub fn to_string(&self, root: RcNode, graphviz: bool, graph_name: &String) -> String {
        let mut bldr = Builder::new(1024);
        let mut to_print = Vec::new();
        to_print.push(root.clone());
        if graphviz {
            bldr.append(format!("subgraph cluster{} {{\n", graph_name));
            bldr.append(format!(" label=\"{}\"\n", graph_name));
        }
        while !to_print.is_empty() {
            let mut children = Vec::new();
            if graphviz {
                bldr.append(format!("{{ rank = same; "));
                for item in &to_print {
                    bldr.append(format!("\"{}_{}\"; ", graph_name, item.borrow().id.0));
                }
                bldr.append(format!("}}\n"));
            }
            for item in &to_print {
                let r_ref = item.borrow();
                if !graphviz {
                    MockNodeStorage::node_as_string(&mut bldr, &r_ref, false, graph_name);
                } else {
                    MockNodeStorage::node_as_string(&mut bldr, &r_ref, graphviz, graph_name);
                    if !r_ref.is_leaf {
                        for d in 0..r_ref.data_count {
                            bldr.append(format!(
                                "{}_{} -> {}_{};",
                                graph_name,
                                r_ref.id.0,
                                graph_name,
                                r_ref.data[d].into_id().0
                            ));
                        }
                    }
                }
                bldr.append(format!("  "));

                if !r_ref.is_leaf {
                    let data = &r_ref.data[0..r_ref.data_count];
                    for id in data.into_iter().map(|x| x.into_id()) {
                        children.push(self.get_node(id).unwrap());
                    }
                }
            }
            //println!("");
            to_print = children;
            if !graphviz {
                println!("");
            }
        }
        if graphviz {
            bldr.append(format!("}}\n"));
        }
        return bldr.string().unwrap();
    }

    fn node_as_string(b: &mut Builder, node: &node::Node, graphviz: bool, graph_name: &String) {
        if graphviz {
            let key_slice = &node.keys[0..node.keys_count];
            let key_as_string = format!("{:?}", key_slice);
            let shape = if node.is_leaf { "box" } else { "ellipse" };
            b.append(format!(
                "{}_{} [label=\"{} \\n {}\" shape=\"{}\"];",
                graph_name, node.id.0, node.id.0, key_as_string, shape
            ));
            if node.right.exists() {
                b.append(format!(
                    "{}_{} -> {}_{};",
                    graph_name, node.id.0, graph_name, node.right.0
                ));
            }

            if node.left.exists() {
                b.append(format!(
                    "{}_{} -> {}_{};",
                    graph_name, node.id.0, graph_name, node.left.0
                ));
            }

            if node.parent.exists() {
                b.append(format!(
                    "{}_{} -> {}_{};",
                    graph_name, node.id.0, graph_name, node.parent.0
                ));
            }
        } else {
            let key_slice = &node.keys[0..node.keys_count];
            let string_data = if node.is_leaf {
                let unpack: Vec<u8> = node
                    .data
                    .iter()
                    .take(node.data_count)
                    .map(|f| f.into_u8())
                    .collect();
                format!("{:?}", unpack)
            } else {
                let unpack: Vec<types::Id> = node
                    .data
                    .iter()
                    .take(node.data_count)
                    .map(|f| f.into_id())
                    .collect();
                format!("{:?}", unpack)
            };
            let left = if node.left.exists() {
                format!("{}", node.left.0)
            } else {
                "_".to_owned()
            };

            let right = if node.right.exists() {
                format!("{}", node.right.0)
            } else {
                "_".to_owned()
            };

            let up = if node.parent.exists() {
                format!("{}", node.parent.0)
            } else {
                "_".to_owned()
            };
            let is_leaf_sfx = if node.is_leaf {
                " ".to_owned()
            } else {
                "*".to_owned()
            };
            b.append(format!(
                "Id:{:?}{}({},{},{})  <{:?}->{}>",
                node.id.0, is_leaf_sfx, left, right, up, key_slice, string_data
            ));
        }
    }
}
#[cfg(test)]
impl NodeStorage for MockNodeStorage {
    fn get_new_id(&self) -> types::Id {
        let max = self.nodes.keys().into_iter().max_by(|x, y| x.cmp(y));
        match max {
            Some(x) => {
                let n = x + 1;
                types::Id(n)
            }
            None => types::Id(1),
        }
    }
    fn get_node(&self, id: Id) -> Result<RcNode, types::Error> {
        let res = self.nodes.get(&id.unwrap());
        if let Some(r) = res {
            Ok(Rc::clone(r))
        } else {
            Err(types::Error(format!("not found Id={}", id.0)))
        }
    }

    fn add_node(&mut self, node: &RcNode) {
        let ref_node = node.borrow();
        self.nodes.insert(ref_node.id.unwrap(), node.clone());
    }

    fn erase_node(&mut self, id: &Id) {
        println!("erase node: Id={}", id.0);
        self.nodes.remove(&id.0);
    }
}
