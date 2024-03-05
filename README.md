# bpts

- [ ] record => {Offset, Value{offset}}
- [ ] const EMPTY_PTR
- [ ] refs by ptr (offset from start), ptr in Node
- [ ] cursor. map over cursor.
- [ ] different 't' for nodes, leafs, root. (tree_settings.default().t_for_leaf(5).min_size_for_root(2)...)
- [ ] logger
- [ ] trait Action - add, scan, remove
- [ ] query language: tablename.where(x => column<3).take(10)
- [ ] bulk loading
- [ ] COW (storage.commit(), storage.rollback())
- [ ] tables
   - [ ] indexes


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