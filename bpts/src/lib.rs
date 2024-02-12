pub mod mfile;
pub mod rec;

use rec::Record;

pub struct Node{
    pub id:u32,
    pub is_leaf:bool,
    pub keys: Vec<i32>,
    pub data: Vec<Record>
}

impl Node{

    pub fn new(is_leaf:bool, keys:Vec<i32>, data:Vec<Record>) -> Node{
        Node { id: 0, is_leaf: is_leaf, keys: keys, data: data }
    }

    pub fn new_leaf(keys:Vec<i32>, data:Vec<Record>) -> Node{
        Node::new(true, keys, data)
    }

    pub fn find(&self, key:i32) -> Option<&Record>{
        for i in 0..self.keys.len(){
            match (self.keys[i]).cmp(&key){
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal => return Some(&self.data[i]),
                std::cmp::Ordering::Greater => return Some(&self.data[i]),
            }
        }
        return None;
    }
}
#[cfg(test)]
mod tests {
    use super::*;

   
    #[test]
    fn leaf_find() {
        let  leaf=Node::new_leaf(vec![1,2,3,4], vec![Record::from_u8(1), Record::from_u8(2),Record::from_u8(3),Record::from_u8(4)]);
        if let Some(item)=leaf.find(2){
            let v= item.into_u8();
            assert_eq!(v, 2u8);
        }
    }
}
