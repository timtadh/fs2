package bptree

import (
	"bytes"
	"log"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/slice"
)

func (self *BpTree) checkValue(value []byte) ([]byte, error) {
	if self.meta.flags&consts.VARCHAR_VALS != 0 {
		v, err := self.varchar.Alloc(len(value))
		if err != nil {
			return nil, err
		}
		err = self.varchar.Do(v, func(data []byte) error {
			copy(data, value)
			return nil
		})
		if err != nil {
			return nil, err
		}
		value = slice.Uint64AsSlice(&v)
	} else if len(value) != int(self.meta.valSize) {
		return nil, errors.Errorf("value was the wrong size")
	}
	return value, nil
}

/* notes on varchar

Going down it is the actual key. Once it gets to the leaf it is
converted to a pointer. The pointer then gets sent back up. Internal
nodes only work with the pointer never with the actual key. That means
when they want to do comparisons they need to indirect into the varchar
store.
*/

// Add a key/value pair to the tree. There is a reason this isn't called
// `Put`, this operation does not replace or modify any data in the
// tree. It only adds this key. The B+ Tree supports duplicate keys and
// even duplicate keys with the same value!
func (self *BpTree) Add(key, value []byte) (err error) {
	if len(key) != int(self.meta.keySize) && self.meta.flags&consts.VARCHAR_KEYS == 0 {
		return errors.Errorf("Key was not the correct size got, %v, expected, %v", len(key), self.meta.keySize)
	}
	value, err = self.checkValue(value)
	if err != nil {
		return err
	}
	cntDelta, root, err := self.add(self.meta.root, key, value, true)
	if err != nil {
		return err
	}
	self.meta.itemCount += cntDelta
	self.meta.root = root
	return self.writeMeta()
}

func (self *BpTree) add(root uint64, key, value []byte, allowDups bool) (cntDelta, newRoot uint64, err error) {
	a, b, err := self.insert(root, key, value, allowDups)
	if err != nil {
		return 0, 0, err
	} else if b == 0 {
		return 1, a, nil
	}
	newRoot, err = self.newInternal()
	if err != nil {
		return 0, 0, err
	}
	err = self.doInternal(newRoot, func(n *internal) error {
		err := self.firstKey(a, func(akey []byte) error {
			return n.putKP(self.varchar, akey, a)
		})
		if err != nil {
			return err
		}
		return self.firstKey(b, func(bkey []byte) error {
			return n.putKP(self.varchar, bkey, b)
		})
	})
	if err != nil {
		return 0, 0, err
	}
	return 1, newRoot, nil
}

/* right is only set on split left is always set.
 * - When split is false left is the pointer to block
 * - When split is true left is the pointer to the new left block
 */
func (self *BpTree) insert(n uint64, key, value []byte, allowDups bool) (a, b uint64, err error) {
	var flags consts.Flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = consts.AsFlag(bytes)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if flags&consts.INTERNAL != 0 {
		return self.internalInsert(n, key, value, allowDups)
	} else if flags&consts.LEAF != 0 {
		return self.leafInsert(n, key, value, allowDups)
	} else {
		return 0, 0, errors.Errorf("Unknown block type")
	}
}

/* - first find the child to insert into
 * - do the child insert
 * - if there was a split:
 *    - if the block is full, split this block
 *    - else insert the new key/pointer into this block
 */
func (self *BpTree) internalInsert(n uint64, key, value []byte, allowDups bool) (a, b uint64, err error) {
	// log.Println("internalInsert", n, key)
	var i int
	var ptr uint64
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
		ptr = *n.ptr(i)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	p, q, err := self.insert(ptr, key, value, allowDups)
	if err != nil {
		return 0, 0, err
	}
	var must_split bool = false
	var split_key []byte = nil
	err = self.doInternal(n, func(m *internal) error {
		*m.ptr(i) = p
		err := self.firstKey(p, func(key []byte) error {
			return m.updateK(self.varchar, i, key)
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
				return m.putKP(self.varchar, key, q)
			})
		}
		return nil
	})
	if err != nil {
		self.doInternal(n, func(n *internal) (err error) {
			log.Println(n.Debug(self.varchar))
			return nil
		})
		log.Printf("n: %v, p: %v, q: %v", n, p, q)
		log.Println(err)
		return 0, 0, err
	}
	if must_split {
		a, b, err = self.internalSplit(n, split_key, q)
		if err != nil {
			return 0, 0, err
		}
	} else {
		a = n
		b = 0
	}
	return a, b, nil
}

func (self *BpTree) newVarcharKey(n uint64, key []byte, allowDups bool) (vkey []byte, err error) {
	var has bool
	err = self.doLeaf(n, func(n *leaf) error {
		var idx int
		idx, has, err = find(self.varchar, n, key)
		if err != nil {
			return err
		}
		if has {
			vkey = make([]byte, n.meta.keySize)
			copy(vkey, n.key(idx))
			return self.varchar.Ref(*slice.AsUint64(&vkey))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if has {
		return vkey, nil
	}
	k, err := self.varchar.Alloc(len(key))
	if err != nil {
		return nil, err
	}
	err = self.varchar.Do(k, func(data []byte) error {
		copy(data, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return slice.Uint64AsSlice(&k), nil
}

func (self *BpTree) leafInsert(n uint64, key, value []byte, allowDups bool) (a, b uint64, err error) {
	var vkey []byte = nil
	if self.meta.flags&consts.VARCHAR_KEYS != 0 {
		vkey, err = self.newVarcharKey(n, key, allowDups)
		if err != nil {
			return 0, 0, err
		}
	}
	if allowDups {
		return self.leafDupAllowInsert(n, vkey, key, value)
	} else {
		return self.leafNoDupInsert(n, vkey, key, value)
	}
}

// Here is the situation. besides the truth table below about which action to
// take several other factors go into this. The first is what exactly do we mean
// when we say a block is "pure". On the surface this means that it only has one
// kind of key. However, this fails at edge cases such as: the block empty, or
// it only has one key. If we take pure to mean the block has all the same keys
// AND there are more than two keys then there is a further problem. If the
// block was once pure and had one or more pure blocks chained after it (in a
// pure run) but now only has one key, then it could be made non-pure by an
// rouge insert. This would result in the items in the chained blocks being
// "lost". The solution is the block is pure if
//
// 1.   All the keys are the same
// 2.   It has at least 1 key
// 3.      ( It has more than 2 keys )
//      OR ( It has less than 3 keys AND a chained block after it )
//
// if empty,
//          then (do the put)
// if   pure,   full,   sameKey
//          then (go to the end and do the put)
// If   pure, ! full,   sameKey
//          then (do the put)
// if   pure, ! sameKey
//          then (do a leafSplit)
// if ! pure,   full
//          then (do a leafSplit)
// if ! pure, ! full,
//          then (do the put)
func (self *BpTree) leafDupAllowInsert(n uint64, vkey, key, value []byte) (a, b uint64, err error) {
	// log.Println("leafDupAllowInsert", n, key)
	var mustSplit bool = false
	var mustPureInsert bool = false
	pure, err := self.isPure(n)
	if err != nil {
		return 0, 0, err
	}
	err = self.doLeaf(n, func(n *leaf) error {
		if n.keyCount() <= 1 {
			return n.put(self.varchar, vkey, key, value)
		}
		full := !n.fitsAnother()
		if pure {
			cmp, err := n.cmpKeyAt(self.varchar, 0, key)
			if err != nil {
				return err
			}
			sameKey := cmp == 0
			if sameKey {
				if full {
					// log.Println("   pure,   full,   sameKey")
					mustPureInsert = true
					return nil
				} else {
					// log.Println("   pure, ! full,   sameKey")
					return n.put(self.varchar, vkey, key, value)
				}
			} else {
				// log.Println("   pure,       , ! sameKey")
				mustSplit = true
				return nil
			}
		} else {
			if full {
				// log.Println(" ! pure, ! full,          ")
				mustSplit = true
				return nil
			} else {
				// log.Println(" ! pure,   full,          ")
				return n.put(self.varchar, vkey, key, value)
			}
		}
	})
	if err != nil {
		return 0, 0, err
	}
	if mustSplit && mustPureInsert {
		return 0, 0, errors.Errorf("cannot do both a pureInsert and a leafSplit")
	} else if mustPureInsert {
		return self.pureLeafInsert(n, vkey, key, value)
	} else if mustSplit {
		return self.leafSplit(n, vkey, key, value)
	}
	return n, 0, nil
}

func (self *BpTree) leafNoDupInsert(n uint64, vkey, key, value []byte) (a, b uint64, err error) {
	// log.Println("leafNoDupInsert", n, key)
	var mustSplit bool = false
	// no need to check for purity as this tree will have unique keys
	err = self.doLeaf(n, func(n *leaf) error {
		if n.keyCount() <= 0 {
			return n.put(self.varchar, vkey, key, value)
		}
		full := !n.fitsAnother()
		idx, has, err := find(self.varchar, n, key)
		if err != nil {
			return err
		} else if has {
			return n.updateValueAt(self.varchar, idx, value)
		} else if full {
			mustSplit = true
			return nil
		} else {
			return n.put(self.varchar, vkey, key, value)
		}
	})
	if err != nil {
		return 0, 0, err
	}
	if mustSplit {
		return self.leafSplit(n, vkey, key, value)
	}
	return n, 0, nil
}

// The block is pure if
//
// 1.   All the keys are the same
// 2.   It has at least 1 key
// 3.      ( It has more than 2 keys )
//      OR ( It has less than 3 keys AND a chained block after it )
//
func (self *BpTree) isPure(a uint64) (pure bool, err error) {
	err = self.doLeaf(a, func(n *leaf) (err error) {
		count := n.keyCount()
		if count == 0 {
			pure = false
			return nil
		} else if n.pure(self.varchar) {
			if count > 2 {
				pure = true
				return nil
			} else {
				run, err := self.pureRun(a)
				if err != nil {
					return err
				}
				if len(run) > 1 {
					pure = true
					return nil
				}
				pure = false
				return nil
			}
		} else {
			pure = false
			return nil
		}
	})
	if err != nil {
		return false, err
	}
	return pure, nil
}

func (self *BpTree) pureLeafInsert(n uint64, vkey, key, value []byte) (a, b uint64, err error) {
	// log.Println("pureLeafInsert", n, key)
	run, err := self.pureRun(n)
	if err != nil {
		return 0, 0, err
	}
	var inserted bool
	for _, a := range run {
		err := self.doLeaf(a, func(n *leaf) error {
			if n.fitsAnother() {
				inserted = true
				return n.put(self.varchar, vkey, key, value)
			}
			return nil
		})
		if err != nil {
			return 0, 0, err
		}
		if inserted {
			break
		}
	}
	if !inserted {
		// we chain on an extra block
		e := run[len(run)-1]
		b, err := self.newLeaf()
		if err != nil {
			return 0, 0, err
		}
		err = self.doLeaf(b, func(m *leaf) error {
			return m.put(self.varchar, vkey, key, value)
		})
		if err != nil {
			return 0, 0, err
		}
		err = self.doLeaf(e, func(o *leaf) error {
			return self.insertListNode(b, e, o.meta.next)
		})
		if err != nil {
			return 0, 0, err
		}
	}
	return n, 0, nil
}

/* On split
 * - first assert that the key to be inserted is not already in the block.
 * - Make a new block
 * - balance the two blocks.
 * - insert the new key/pointer combo into the correct block
 *
 * Note. in the varchar case, the key is not the key but a pointer to a
 * key. This complicates the bytes.Compare line significantly.
 */
func (self *BpTree) internalSplit(n uint64, key []byte, ptr uint64) (a, b uint64, err error) {
	// log.Println("internalSplit", n, key)
	a = n
	b, err = self.newInternal()
	if err != nil {
		return 0, 0, err
	}
	err = self.doInternal(a, func(n *internal) error {
		return self.doInternal(b, func(m *internal) (err error) {
			err = n.balance(self.varchar, m)
			if err != nil {
				return err
			}
			if self.meta.flags&consts.VARCHAR_KEYS == 0 {
				if bytes.Compare(key, m.key(0)) < 0 {
					return n.putKP(self.varchar, key, ptr)
				} else {
					return m.putKP(self.varchar, key, ptr)
				}
			} else {
				return self.varchar.Do(*slice.AsUint64(&key), func(k []byte) error {
					return m.doKeyAt(self.varchar, 0, func(m_key_0 []byte) error {
						if bytes.Compare(k, m_key_0) < 0 {
							return n.putKP(self.varchar, key, ptr)
						} else {
							return m.putKP(self.varchar, key, ptr)
						}
					})
				})
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
func (self *BpTree) leafSplit(n uint64, vkey, key, value []byte) (a, b uint64, err error) {
	// log.Println("leafSplit", n, key)
	var isPure bool = false
	a = n
	err = self.doLeaf(a, func(n *leaf) (err error) {
		isPure = n.pure(self.varchar)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if isPure {
		return self.pureLeafSplit(n, vkey, key, value)
	}
	b, err = self.newLeaf()
	if err != nil {
		return 0, 0, err
	}
	err = self.doLeaf(a, func(n *leaf) (err error) {
		err = self.insertListNode(b, a, n.meta.next)
		if err != nil {
			return err
		}
		return self.doLeaf(b, func(m *leaf) (err error) {
			err = n.balance(self.varchar, m)
			if err != nil {
				return err
			}
			return m.doKeyAt(self.varchar, 0, func(mk []byte) error {
				if self.meta.flags&consts.VARCHAR_KEYS != 0 {
					if bytes.Compare(key, mk) < 0 {
						return n.putKV(self.varchar, vkey, value)
					} else {
						return m.putKV(self.varchar, vkey, value)
					}
				} else {
					if bytes.Compare(key, mk) < 0 {
						return n.putKV(self.varchar, key, value)
					} else {
						return m.putKV(self.varchar, key, value)
					}
				}
			})
		})
	})
	if err != nil {
		return 0, 0, err
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
func (self *BpTree) pureLeafSplit(n uint64, vkey, key, value []byte) (a, b uint64, err error) {
	// log.Println("pureLeafSplit", n, key)
	var unneeded bool = false
	new_off, err := self.newLeaf() // this needs to be hoisted!!
	if err != nil {
		return 0, 0, err
	}
	err = self.doLeaf(n, func(node *leaf) (err error) {
		return node.doKeyAt(self.varchar, 0, func(node_key_0 []byte) error {
			if bytes.Compare(key, node_key_0) < 0 {
				a = new_off
				b = n
				err = self.insertListNode(a, node.meta.prev, b)
				if err != nil {
					return err
				}
				return self.doLeaf(a, func(anode *leaf) (err error) {
					if self.meta.flags&consts.VARCHAR_KEYS != 0 {
						return anode.putKV(self.varchar, vkey, value)
					} else {
						return anode.putKV(self.varchar, key, value)
					}
				})
			} else {
				a = n
				e, err := self.endOfPureRun(a)
				if err != nil {
					return err
				}
				return self.doLeaf(e, func(m *leaf) (err error) {
					err = node.doKeyAt(self.varchar, 0, func(a_key_0 []byte) error {
						return m.doKeyAt(self.varchar, 0, func(e_key_0 []byte) error {
							if !bytes.Equal(a_key_0, e_key_0) {
								log.Println("a", a, node.Debug(self.varchar))
								log.Println("e", e, m.Debug(self.varchar))
								log.Println("went off of end of pure run")
								return errors.Errorf("End of pure run went off of pure run")
							}
							if m.meta.next == 0 {
								return nil
							}
							return self.doLeaf(m.meta.next, func(o *leaf) error {
								return o.doKeyAt(self.varchar, 0, func(o_key_0 []byte) error {
									if bytes.Equal(a_key_0, o_key_0) {
										log.Println("a", a, node.Debug(self.varchar))
										log.Println("e", e, m.Debug(self.varchar))
										log.Println("e.meta.next", m.meta.next, o.Debug(self.varchar))
										log.Println("did not find end of pure run")
										return errors.Errorf("did not find end of pure run")
									}
									return nil
								})
							})
						})
					})
					if err != nil {
						return err
					}
					return m.doKeyAt(self.varchar, 0, func(m_key_0 []byte) error {
						if m.fitsAnother() && bytes.Equal(key, m_key_0) {
							unneeded = true
							if self.meta.flags&consts.VARCHAR_KEYS != 0 {
								return m.putKV(self.varchar, vkey, value)
							} else {
								return m.putKV(self.varchar, key, value)
							}
						} else {
							return self.doLeaf(new_off, func(o *leaf) (err error) {
								if self.meta.flags&consts.VARCHAR_KEYS != 0 {
									err = o.putKV(self.varchar, vkey, value)
								} else {
									err = o.putKV(self.varchar, key, value)
								}
								if err != nil {
									return err
								}
								if bytes.Compare(key, m_key_0) >= 0 {
									err = self.insertListNode(new_off, e, m.meta.next)
								} else {
									err = self.insertListNode(new_off, m.meta.prev, e)
								}
								if err != nil {
									return err
								}
								b = 0
								if !bytes.Equal(key, node_key_0) {
									b = new_off
								}
								return nil
							})
						}
					})
				})
			}
		})
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

func (self *BpTree) endOfPureRun(start uint64) (uint64, error) {
	run, err := self.pureRun(start)
	if err != nil {
		return 0, err
	}
	return run[len(run)-1], nil
}

func (self *BpTree) pureRun(start uint64) ([]uint64, error) {
	/* this algorithm was pretty tricky to port do to the "interesting"
	 * memory management scheme that I am employing (everything inside
	 * of "do" methods). Basically, the problem was it was mutating
	 * variables which I can not mutate because they are stuck inside of
	 * the do methods. I had to find a way to hoist just the information
	 * I needed.
	 */
	var key []byte
	err := self.doLeaf(start, func(n *leaf) error {
		if n.meta.keyCount < 0 {
			return errors.Errorf("block was empty")
		}
		return n.doKeyAt(self.varchar, 0, func(k []byte) error {
			key = make([]byte, len(k))
			copy(key, k)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	getNext := func(a uint64) (next uint64, err error) {
		err = self.doLeaf(a, func(n *leaf) error {
			next = n.meta.next
			return nil
		})
		return next, err
	}
	isValid := func(a uint64) (valid bool, err error) {
		if a == 0 {
			return false, nil
		}
		err = self.doLeaf(a, func(n *leaf) error {
			return n.doKeyAt(self.varchar, 0, func(k []byte) error {
				valid = bytes.Equal(key, k)
				return nil
			})
		})
		return valid, err
	}
	cur := start
	valid, err := isValid(cur)
	if err != nil {
		return nil, err
	}
	run := make([]uint64, 0, 10)
	for valid {
		run = append(run, cur)
		cur, err = getNext(cur)
		if err != nil {
			return nil, err
		}
		valid, err = isValid(cur)
		if err != nil {
			return nil, err
		}
	}
	return run, nil
}
