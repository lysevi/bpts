# bpts

- [ ] bpts/utils/[mod.rs, bufferwriter]
- [ ] Page is full.
  - [x] data list
  - [ ] is there enough space for recording?
  - [ ] transaction...key+value
  - [ ] page as storage
  - [ ] test: single tree, many transactions: insert+read+remove+read
  - [ ] test: many trees, many transactions: insert+read+remove+read
- [ ] transaction is NodeStorage. 
  - [ ] Lazy load - method "load" to load data from buffer.
  - [x] unload after saving
  - [x] save to buffer.
  - [x] test - fill, save, load, remove, save.... 

- [x] different 't' for nodes, leafs, root. (tree_settings.default().t_for_leaf(5).min_size_for_root(2)...)
   - [ ] key comparer
   - [ ] key - Vec<u8>. 
- [ ] logger
- [ ] query language: tablename.where(x => column<3).take(10)
- [ ] bulk loading
- [ ] COW (storage.commit(), storage.rollback())
   - [ ] Meta-page store all tree. link to prev. meta-page
- [ ] tables
   - [ ] indexes


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