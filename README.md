# bpts

- [ ] bpts/utils/[mod.rs, bufferwriter]
- [ ] Page is full.
  - [ ] data list
  - [ ] is there enough space for recording?
  - [ ] transaction...key+value
  - [ ] page as storage
- [ ] transaction is NodeStorage. 
  - [ ] Lazy load - method "load" to load data from buffer.
  - [ ] unload after saving
  - [*] save to buffer.
  - [ ] test - fill, save, load, remove, save.... 

- [ ] different 't' for nodes, leafs, root. (tree_settings.default().t_for_leaf(5).min_size_for_root(2)...)
   - [x] key comparer
   - [ ] key - Vec<u8>. 
- [ ] logger
- [ ] trait Action - add, scan, remove
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
- [*] test: take from high leaf with different parrents
- [*] test: take from low leaf with different parrents
- [*] test: take from low node with different parrents
- [*] test: take from high node with different parrents