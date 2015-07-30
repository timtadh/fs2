package bptree

import (
	"log"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/slice"
)

// Remove one or more key/value pairs at the given key. The callback
// `where` will be called for each pair encountered and the value will
// be passed into the callback. If `where` returns true the item is
// removed otherwise it is left unchanged. To remove all items with a
// particular key simply:
//
// 	err = bpt.Remove(key, func(value []byte) bool { return true })
// 	if err != nil {
// 		panic(err)
// 	}
//
func (self *BpTree) Remove(key []byte, where func([]byte) bool) (err error) {
	log.Println("removing", key)
	err = self.Verify()
	if err != nil {
		return err
	}
	a, err := self.remove(0, self.meta.root, 0, key, where)
	if err != nil {
		return err
	}
	if a == 0 {
		a, err = self.newLeaf()
		if err != nil {
			return err
		}
	}
	self.meta.itemCount -= 1
	self.meta.root = a
	err = self.writeMeta()
	if err != nil {
		return err
	}
	return self.Verify()
}

func (self *BpTree) remove(parent, n, sibling uint64, key []byte, where func([]byte) bool) (a uint64, err error) {
	var flags consts.Flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = consts.AsFlag(bytes)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if flags&consts.INTERNAL != 0 {
		return self.internalRemove(parent, n, sibling, key, where)
	} else if flags&consts.LEAF != 0 {
		return self.leafRemove(parent, n, sibling, key, where)
	} else {
		return 0, errors.Errorf("Unknown block type")
	}
}

func (self *BpTree) internalRemove(parent, n, sibling uint64, key []byte, where func([]byte) bool) (a uint64, err error) {
	// log.Println("internalRemove", n, key)
	var i int
	var kid uint64
	err = self.doInternal(n, func(n *internal) (err error) {
		var has bool
		i, has, err = find(self.varchar, n, key)
		if err != nil {
			return err
		}
		if !has && i > 0 {
			// if it doesn't have it and the index > 0 then we have the
			// next block so we have to subtract one from the index.
			i--
		}
		kid = *n.ptr(i)
		if i+1 < int(n.meta.keyCount) {
			sibling = *n.ptr(i + 1)
		} else if sibling != 0 {
			return self.doInternal(sibling, func(m *internal) error {
				sibling = *m.ptr(0)
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	okid := kid
	kid, err = self.remove(n, kid, sibling, key, where)
	if err != nil {
		self.doInternal(n, func(n *internal) (err error) {
			log.Println(n.Debug(self.varchar))
			return nil
		})
		log.Printf("n: %v, sibling: %v, okid %v,", n, sibling, okid)
		log.Println(err)
		return 0, err
	}
	if kid == 0 {
		err = self.doInternal(n, func(n *internal) error {
			return n.delItemAt(self.varchar, i)
		})
		if err != nil {
			return 0, err
		}
	} else {
		err = self.doInternal(n, func(n *internal) error {
			*n.ptr(i) = kid
			return self.firstKey(kid, func(kid_key []byte) error {
				return n.updateK(self.varchar, i, kid_key)
			})
		})
		if err != nil {
			self.doInternal(n, func(n *internal) (err error) {
				log.Println(n.Debug(self.varchar))
				return nil
			})
			log.Printf("n: %v, sibling: %v, okid %v, kid: %v", n, sibling, okid, kid)
			log.Println(err)
			return 0, err
		}
	}
	var keyCount uint16
	err = self.doInternal(n, func(n *internal) error {
		keyCount = n.meta.keyCount
		return nil
	})
	if err != nil {
		return 0, err
	}
	if keyCount == 0 {
		return 0, nil
	}
	return n, nil
}

func (self *BpTree) leafRemove(parent, n, sibling uint64, key []byte, where func([]byte) bool) (b uint64, err error) {
	// log.Println("leafRemove", n, key)
	start := n
	a := n
	var i int
	var has bool
	err = self.doLeaf(a, func(n *leaf) error {
		i, has, err = find(self.varchar, n, key)
		return err
	})
	if err != nil {
		return 0, err
	}
	if !has {
		self.doLeaf(n, func(n *leaf) error {
			log.Println(a, n.Debug(self.varchar))
			if n.meta.next != 0 {
				self.doLeaf(n.meta.next, func(n *leaf) error {
					log.Println("n.meta.next", n.Debug(self.varchar))
					return nil
				})
			}
			if sibling != 0 {
				self.doLeaf(sibling, func(n *leaf) error {
					log.Println("sibing", sibling, n.Debug(self.varchar))
					return nil
				})
			}
			if parent != 0 {
				self.do(parent,
					func(n *internal) error {
						log.Println("parent", parent, n.Debug(self.varchar))
						return nil
					},
					func(n *leaf) error {
						log.Println("parent", parent, n.Debug(self.varchar))
						return nil
					},
				)
			}
			return nil
		})
		log.Printf("n = %v, sibling = %v, node did not have key %v", n, sibling, key)
		a, i, err = self.leafGetStart(n, key, true, sibling)
		if err != nil {
			log.Println("could not find key with get start", key)
			return 0, err
		}
	}
	next, err := self.forwardFrom(a, i, key)
	if err != nil {
		return 0, err
	}
	b = a
	type loc struct {
		a uint64
		i int
	}
	locs := make([]*loc, 0, 10)
	for a, i, err, next = next(); next != nil; a, i, err, next = next() {
		locs = append(locs, &loc{a, i})
	}
	if err != nil {
		return 0, err
	}
	for x := len(locs) - 1; x >= 0; x-- {
		a := locs[x].a
		i := locs[x].i
		var vi uint64
		var remove bool = false
		err = self.doLeaf(a, func(n *leaf) error {
			err = n.doValueAt(self.varchar, i, func(value []byte) error {
				remove = where(value)
				return nil
			})
			if err != nil {
				return err
			}
			if remove {
				if self.meta.flags&consts.VARCHAR_VALS != 0 {
					v := n.val(i)
					vi = *slice.AsUint64(&v)
				}
				err = n.delItemAt(self.varchar, i)
				if err != nil {
					return err
				}
			}
			if int(n.meta.keyCount) <= 0 {
				next := n.meta.next
				if a == start {
					if next == 0 {
						b = 0
					} else if next != sibling {
						b = next
					} else {
						b = 0
					}
				}
				err = self.delListNode(a)
				if err != nil {
					return err
				}
				err = self.bf.Free(a)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		if remove && vi != 0 && self.meta.flags&consts.VARCHAR_VALS != 0 {
			err = self.varchar.Deref(vi)
			if err != nil {
				return 0, err
			}
		}
	}
	if b == sibling && sibling != 0 {
		count := 0
		err = self.doLeaf(start, func(n *leaf) error {
			count = n.keyCount()
			return nil
		})
		if err != nil {
			return 0, err
		}
		if count == 0 {
			return 0, nil
		} else {
			return start, nil
		}
	}
	return b, nil
}
