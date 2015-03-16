package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/errors"
)

// Add a key/value pair to the tree. There is a reason this isn't called
// `Put`, this operation does not replace or modify any data in the
// tree. It only adds this key. The B+ Tree supports duplicate keys and
// even duplicate keys with the same value!
func (self *BpTree) Add(key, value []byte) error {
	if len(key) != int(self.meta.keySize) {
		return errors.Errorf("Key was not the correct size got, %v, expected, %v", len(key), self.meta.keySize)
	}
	a, b, c, err := self.insert(self.meta.root, key, value)
	if err != nil {
		return err
	} else if b == 0 {
		self.meta.itemCount += 1
		self.meta.root = a
		return self.writeMeta()
	}
	root, err := self.newInternal()
	if err != nil {
		return err
	}
	err = self.doInternal(root, func(n *internal) error {
		err := self.firstKey(a, func(akey []byte) error {
			return n.putKP(akey, a)
		})
		if err != nil {
			return err
		}
		err = self.firstKey(b, func(bkey []byte) error {
			return n.putKP(bkey, b)
		})
		if err != nil {
			return err
		}
		if c != 0 {
			return self.firstKey(c, func(ckey []byte) error {
				return n.putKP(ckey, c)
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	self.meta.itemCount += 1
	self.meta.root = root
	return self.writeMeta()
}

/* right is only set on split left is always set.
 * - When split is false left is the pointer to block
 * - When split is true left is the pointer to the new left block
 */
func (self *BpTree) insert(n uint64, key, value []byte) (a, b, c uint64, err error) {
	var flags Flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = Flag(bytes[0])
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if flags&iNTERNAL != 0 {
		return self.internalInsert(n, key, value)
	} else if flags&lEAF != 0 {
		return self.leafInsert(n, key, value)
	} else {
		return 0, 0, 0, errors.Errorf("Unknown block type")
	}
}

/* - first find the child to insert into
 * - do the child insert
 * - if there was a split:
 *    - if the block is full, split this block
 *    - else insert the new key/pointer into this block
 */
func (self *BpTree) internalInsert(n uint64, key, value []byte) (a, b, c uint64, err error) {
	var i int
	var ptr uint64
	err = self.doInternal(n, func(n *internal) error {
		var has bool
		i, has = find(n, key)
		if !has && i > 0 {
			// if it doesn't have it and the index > 0 then we have the
			// next block so we have to subtract one from the index.
			i--
		}
		ptr = *n.ptr(i)
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	p, q, r, err := self.insert(ptr, key, value)
	if err != nil {
		return 0, 0, 0, err
	}
	var must_split bool = false
	var split_key []byte = nil
	err = self.doInternal(n, func(m *internal) error {
		*m.ptr(i) = p
		err := self.firstKey(p, func(key []byte) error {
			copy(m.key(i), key)
			return nil
		})
		if err != nil {
			return err
		}
		if q != 0 {
			return self.firstKey(q, func(key []byte) error {
				if m.full() {
					must_split = true
					split_key = make([]byte, len(key))
					copy(split_key, key)
					return nil
				}
				return m.putKP(key, q)
			})
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if must_split {
		a, b, err = self.internalSplit(n, split_key, q)
		if err != nil {
			return 0, 0, 0, err
		}
	} else {
		a = n
		b = 0
	}
	if r != 0 {
		var must_split bool = false
		var split_key []byte = nil
		err = self.doInternal(a, func(n *internal) error {
			if b != 0 {
				return self.doInternal(b, func(m *internal) error {
					return self.firstKey(r, func(rkey []byte) error {
						if bytes.Compare(rkey, m.key(0)) < 0 {
							// goes into a
							return n.putKP(rkey, r)
						} else {
							// goes into b
							return m.putKP(rkey, r)
						}
					})
				})
			}
			return self.firstKey(r, func(rkey []byte) error {
				if n.full() {
					must_split = true
					split_key = make([]byte, len(rkey))
					copy(split_key, rkey)
					return nil
				}
				return n.putKP(rkey, r)
			})
		})
		if err != nil {
			return 0, 0, 0, err
		}
		if must_split {
			a, b, err = self.internalSplit(a, split_key, r)
			if err != nil {
				return 0, 0, 0, err
			}
		}
	}
	return a, b, 0, nil
}

func (self *BpTree) leafInsert(n uint64, key, value []byte) (a, b, c uint64, err error) {
	return self.leafDoInsert(n, key, value)
}

func (self *BpTree) leafDoInsert(n uint64, key, value []byte) (a, b, c uint64, err error) {
	var mustSplit bool = false
	err = self.doLeaf(n, func(n *leaf) error {
		if !n.fitsAnother() {
			mustSplit = true
			return nil
		}
		return n.putKV(key, value)
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if mustSplit {
		return self.leafSplit(n, key, value)
	}
	return n, 0, 0, nil
}

/* On split
 * - first assert that the key to be inserted is not already in the block.
 * - Make a new block
 * - balance the two blocks.
 * - insert the new key/pointer combo into the correct block
 */
func (self *BpTree) internalSplit(n uint64, key []byte, ptr uint64) (a, b uint64, err error) {
	a = n
	b, err = self.newInternal()
	if err != nil {
		return 0, 0, err
	}
	err = self.doInternal(a, func(n *internal) error {
		return self.doInternal(b, func(m *internal) (err error) {
			err = n.balance(m)
			if err != nil {
				return err
			}
			if bytes.Compare(key, m.key(0)) < 0 {
				return n.putKP(key, ptr)
			} else {
				return m.putKP(key, ptr)
			}
		})
	})
	if err != nil {
		return 0, 0, err
	}
	return a, b, nil
}

/* on leaf split if the block is pure then it will defer to
 * pure_leaf_split else
 * - a new block will be made and inserted after this one
 * - the two blocks will be balanced with balanced_nodes
 * - if the key is less than b.keys[0] it will go in a else b
 */
func (self *BpTree) leafSplit(n uint64, key, value []byte) (a, b, c uint64, err error) {
	var isPure bool = false
	a = n
	err = self.doLeaf(a, func(n *leaf) (err error) {
		isPure = n.pure()
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if isPure {
		a, b, err = self.pureLeafSplit(n, key, value)
		return a, b, 0, err
	}
	b, err = self.newLeaf()
	if err != nil {
		return 0, 0, 0, err
	}
	c_unneeded := true
	ret_c := false
	c, err = self.newLeaf()
	if err != nil {
		return 0, 0, 0, err
	}
	d_unneeded := true
	d, err := self.newLeaf()
	if err != nil {
		return 0, 0, 0, err
	}
	err = self.doLeaf(a, func(n *leaf) (err error) {
		err = self.insertListNode(b, a, n.meta.next)
		if err != nil {
			return err
		}
		return self.doLeaf(b, func(m *leaf) (err error) {
			err = n.balance(m)
			if err != nil {
				return err
			}
			if bytes.Compare(key, m.key(0)) < 0 {
				return n.putKV(key, value)
			} else {
				return m.putKV(key, value)
			}
		})
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if c_unneeded {
		err = self.bf.Free(c)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	if d_unneeded {
		err = self.bf.Free(d)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	if ret_c && !c_unneeded {
		return a, b, c, nil
	}
	return a, b, 0, nil
}

/* a pure leaf split has two cases:
 *  1) the inserted key is less than the current pure block.
 *     - a new block should be created before the current block
 *     - the key should be put in it
 *  2) the inserted key is greater than or equal to the pure block.
 *     - the end of run of pure blocks should be found
 *     - if the key is equal to pure block and the last block is not
 *       full insert the new kv
 *     - else split by making a new block after the last block in the run
 *       and putting the new key there.
 *     - always return the current block as "a" and the new block as "b"
 */
func (self *BpTree) pureLeafSplit(n uint64, key, value []byte) (a, b uint64, err error) {
	var unneeded bool = false
	new_off, err := self.newLeaf() // this needs to be hoisted!!
	if err != nil {
		return 0, 0, err
	}
	err = self.doLeaf(n, func(node *leaf) (err error) {
		if bytes.Compare(key, node.key(0)) < 0 {
			a = new_off
			b = n
			err = self.insertListNode(a, node.meta.prev, b)
			if err != nil {
				return err
			}
			return self.doLeaf(a, func(anode *leaf) (err error) {
				return anode.putKV(key, value)
			})
		} else {
			a = n
			e, err := self.endOfPureRun(a)
			if err != nil {
				return err
			}
			return self.doLeaf(e, func(m *leaf) (err error) {
				if m.fitsAnother() && bytes.Equal(key, m.key(0)) {
					unneeded = true
					return m.putKV(key, value)
				} else {
					return self.doLeaf(new_off, func(o *leaf) (err error) {
						err = o.putKV(key, value)
						if err != nil {
							return err
						}
						if bytes.Compare(key, m.key(0)) >= 0 {
							err = self.insertListNode(new_off, e, m.meta.next)
						} else {
							err = self.insertListNode(new_off, m.meta.prev, e)
						}
						if err != nil {
							return err
						}
						b = 0
						if !bytes.Equal(key, node.key(0)) {
							b = new_off
						}
						return nil
					})
				}
			})
		}
	})
	if err != nil {
		return 0, 0, err
	}
	if unneeded {
		err = self.bf.Free(new_off)
		if err != nil {
			return 0, 0, err
		}
	}
	return a, b, nil
}

func (self *BpTree) endOfPureRun(start uint64) (a uint64, err error) {
	/* this algorithm was pretty tricky to port do to the "interesting"
	 * memory management scheme that I am employing (everything inside
	 * of "do" methods). Basically, the problem was it was mutating
	 * variables which I can not mutate because they are stuck inside of
	 * the do methods. I had to find a way to hoist just the information
	 * I needed.
	 */
	err = self.doLeaf(start, func(n *leaf) error {
		if n.meta.keyCount < 0 {
			return errors.Errorf("block was empty")
		}
		key := n.key(0)
		prev := start
		next := n.meta.next
		notDone := func(next uint64, node *leaf) bool {
			return next != 0 && node.pure() && bytes.Equal(key, node.key(0))
		}
		not_done := notDone(next, n)
		for not_done {
			err = self.doLeaf(next, func(n *leaf) error {
				prev = next
				next = n.meta.next
				not_done = notDone(next, n)
				return nil
			})
			if err != nil {
				return err
			}
		}
		a = prev
		return nil
	})
	if err != nil {
		return 0, err
	}
	return a, nil
}
