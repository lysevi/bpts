# bpts

- [ ] Page is full.
  - [ ] crc
  - [ ] bloom
- [ ] Storage
  - [ ] datablockheader.is_full
- [ ] benchmark on memory sorage
  - [ ] median time
  - [ ] mean time
  - [ ] 99 percentile time
- [ ] transaction is NodeStorage. 
  - [ ] Lazy load - method "load" to load data from buffer.
- [ ] - binary search in nodes.
  - [ ] - benchmark - node::find_key
  - [ ] - benchmark - node::find
  - [ ] - benchmark - split::insert_key_to_parent
  - [ ] - criterion
- [ ] - Record is a array with common type.
- [ ] single transaction for many trees.
- [ ] logger
- [ ] query language: tablename.where(x => column<3).take(10)
- [ ] bulk loading
- [ ] COW (storage.commit(), storage.rollback())
   - [ ] Meta-page store all tree. link to prev. meta-page
- [ ] tables
   - [ ] indexes
- [ ] - bloom in transaction
- [ ] remove over insert special flag.