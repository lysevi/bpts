# bpts

- [ ] Page is full.
  - [x] clusters
   - [x] free clusters list
   - [x] free mem after reccord
   - [x] free data clusters.
  - [ ] remove over insert special flag.
  - [x] write data into page.
  - [x] data list
  - [x] data list -> data loader
  - [x] data list - remove offset. write over clusters.
  - [x] is there enough space for recording?
  
  - [ ] page as storage
  - [ ] test: single tree, many transactions:
    - [x] insert
    - [x] find
    - [x] remove

  - [ ] test: many trees, many transactions:     
    - [ ] insert
    - [ ] find
    - [ ] remove
  - [ ] crc
- [ ] transaction is NodeStorage. 
  - [ ] Lazy load - method "load" to load data from buffer.
  - [x] unload after saving
  - [x] save to buffer.
  - [x] test - fill, save, load, remove, save.... 
- [ ] - bpts_tree is a submodule of bpts
- [ ] - bpts_tree::types::Error - in lib.rs
- [ ] - binary search in nodes.
  - [ ] - benchmark - node::find_key
  - [ ] - benchmark - node::find
  - [ ] - benchmark - split::insert_key_to_parent
- [ ] single transaction for many trees.
- [ ] logger
- [ ] query language: tablename.where(x => column<3).take(10)
- [ ] bulk loading
- [ ] COW (storage.commit(), storage.rollback())
   - [ ] Meta-page store all tree. link to prev. meta-page
- [ ] tables
   - [ ] indexes
- [ ] - bloom in transaction



- [x] cursor. map over cursor.
- [x] remove
- [x] remove from middle range
- [x] range query
- [x] const EMPTY_ID
- [x] mock node storage
- [x] insertion
- [x] link to parent
- [x] link to brother (left, right)
- [x] test: take from high leaf with different parrents
- [x] test: take from low leaf with different parrents
- [x] test: take from low node with different parrents
- [x] test: take from high node with different parrents