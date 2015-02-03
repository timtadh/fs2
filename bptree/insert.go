package bptree

import (
	"bytes"
)


func (self *BpTree) Put(key, value []byte) error {
	if len(key) != int(self.meta.keySize) {
		return Errorf("Key was not the correct size got, %v, expected, %v", len(key), self.meta.keySize)
	}
	a, b, err := self.insert(self.meta.root, key, value)
	if err != nil {
		return err
	} else if b == 0 {
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
		return self.firstKey(b, func(bkey []byte) error {
			return n.putKP(bkey, b)
		})
	})
	if err != nil {
		return err
	}
	self.meta.root = root
	return nil
}


/* right is only set on split left is always set. 
 * - When split is false left is the pointer to block
 * - When split is true left is the pointer to the new left block
 */
func (self *BpTree) insert(n uint64, key, value []byte) (a, b uint64, err error) {
	var flags flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = flag(bytes[0])
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if flags & INTERNAL != 0 {
		return self.internalInsert(n, key, value)
	} else if flags & LEAF != 0 {
		return self.leafInsert(n, key, value)
	} else if flags & BIG_LEAF != 0 {
		return self.bigLeafInsert(n, key, value)
	} else {
		return 0, 0, Errorf("Unknown block type")
	}
}

func (self *BpTree) internalInsert(n uint64, key, value []byte) (a, b uint64, err error) {
	return 0, 0, Errorf("unimplemented")
}

func (self *BpTree) leafInsert(n uint64, key, value []byte) (a, b uint64, err error) {
	if len(value) > self.bf.BlockSize()>>2 {
		return self.leafInsertBigValue(n, key, value)
	}
	var mustSplit bool = false
	err = self.doLeaf(n, func(n *leaf) error {
		if !n.fits(value) {
			mustSplit = true
			return nil
		}
		return n.putKV(key, value)
	})
	if err != nil {
		return 0, 0, err
	}
	if mustSplit {
		return self.leafSplit(n, key, value)
	}
	return n, 0, nil
}

func (self *BpTree) bigLeafInsert(n uint64, key, value []byte) (a, b uint64, err error) {
	return 0, 0, Errorf("unimplemented")
}

func (self *BpTree) leafInsertBigValue(n uint64, key, value []byte) (a, b uint64, err error) {
	return 0, 0, Errorf("unimplemented")
}


func (self *BpTree) internalSplit(n uint64, key, value []byte) (a, b uint64, err error) {
	return 0, 0, Errorf("unimplemented")
}


/* on leaf split if the block is pure then it will defer to
 * pure_leaf_split else
 * - a new block will be made and inserted after this one
 * - the two blocks will be balanced with balanced_nodes
 * - if the key is less than b.keys[0] it will go in a else b
 */
func (self *BpTree) leafSplit(n uint64, key, value []byte) (a, b uint64, err error) {
	var isPure bool = false
	a = n
	err = self.doLeaf(a, func(n *leaf) (err error) {
		if n.pure() {
			isPure = true
			return nil
		}
		// TIM YOU NEED TO HOIST THIS NEW CALL.
		// since it is allocating you need to leave the do context.
		// You then need to enter a new do context since you will
		// need use a reference to n.
		b, err = self.newLeaf()
		if err != nil {
			return err
		}
		err = insertListNode(self.bf, b, a, n.meta.next)
		if err != nil {
			return err
		}
		return self.doLeaf(b, func(m *leaf) (err error) {
			err = n.balance(m)
			if err != nil {
				return err
			}
			if bytes.Compare(key, m.keys[0]) < 0 {
				return n.putKV(key, value)
			} else {
				return m.putKV(key, value)
			}
		})
	})
	if err != nil {
		return 0, 0, nil
	}
	if isPure {
		return self.pureLeafSplit(n, key, value)
	}
	return a, b, nil
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
	err = self.doLeaf(n, func(node *leaf) (err error) {
		if bytes.Compare(key, node.keys[0]) < 0 {
			b = n
			a, err = newLeaf() // this needs to be hoisted!!
			if err != nil {
				return err
			}
			err = insertListNode(self.bf, a, node.meta.prev, b)
			if err != nil {
				return err
			}
			return self.doLeaf(a, func(anode *leaf) (err error) {
				return anode.putKV(key, value)
			})
		} else {
			a = n
			e, err = endOfPureRun(a)
			if err != nil {
				return err
			}
			// TIM YOU ARE WRITING HERE
		}
	})
	if err != nil {
		return 0, 0, err
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
			return Errorf("block was empty")
		}
		key := n.keys[0]
		prev := start
		next := n.meta.next
		notDone := func(next uint64, node *leaf) bool {
			 return next != 0 && node.pure() && bytes.Equal(key, node.keys[0])
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

