package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
)

// This type of iterator is used for iterating over keys OR values.
type Iterator func() (item []byte, err error, i Iterator)

// This type of iterator is used for iterating over keys AND values.
type KVIterator func() (key, value []byte, err error, kvi KVIterator)

type bpt_iterator func() (a uint64, idx int, err error, bi bpt_iterator)

func doIter(run func() (KVIterator, error), do func(key, value []byte) error) error {
	kvi, err := run()
	if err != nil {
		return err
	}
	var key, value []byte
	for key, value, err, kvi = kvi(); kvi != nil; key, value, err, kvi = kvi() {
		do(key, value)
	}
	return err
}

func doItemIter(run func() (Iterator, error), do func([]byte) error) error {
	it, err := run()
	if err != nil {
		return err
	}
	var item []byte
	for item, err, it = it(); it != nil; item, err, it = it() {
		do(item)
	}
	return err
}

// Iterate over all of the key/value pairs in the tree
//
// 	err = bpt.DoIterate(func(key, value []byte) error {
// 		// do something with each key and value in the tree
// 	})
// 	if err != nil {
// 		// handle error
// 	}
//
// Note, it is safe for the keys and values to escape the `do` context.
// They are copied into it so you cannot harm the tree. An unsafe
// version of this is being considered.
func (self *BpTree) DoIterate(do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Iterate() },
		do,
	)
}

// Iterate over each of the keys and values in the tree. I recommend
// that you use the `DoIterate` method instead (it is easier to use). If
// you do use the method always use it as follows:
//
// 	kvi, err := bpt.Iterate()
// 	if err != nil {
// 		// handle error
// 	}
// 	var key, value []byte // must be declared here
// 	// do not use a := assign here only a =
// 	for key, value, err, kvi = kvi(); kvi != nil; key, value, err, kvi = kvi() {
// 		// do something with each key and value
// 	}
// 	// now the iterator could have exited with an error so check the
// 	// error before continuing
// 	if err != nil {
// 		// handle error
// 	}
//
// Note, it is safe for the keys and values to escape the iterator
// context.  They are copied into it so you cannot harm the tree. An
// unsafe version of this is being considered.
func (self *BpTree) Iterate() (kvi KVIterator, err error) {
	return self.Range(nil, nil)
}

// Iterate over all of the keys in the tree. See DoIterate() for usage
// details
func (self *BpTree) DoKeys(do func([]byte) error) error {
	return doItemIter(
		func() (Iterator, error) { return self.Keys() },
		do,
	)
}

// Iterate over all of the keys in the tree. See Iterate() for usage
// details
func (self *BpTree) Keys() (it Iterator, err error) {
	kvi, err := self.Iterate()
	if err != nil {
		return nil, err
	}
	var pk []byte
	it = func() (key []byte, err error, _it Iterator) {
		for key == nil || bytes.Equal(pk, key) {
			key, _, err, kvi = kvi()
			if err != nil {
				return nil, err, nil
			}
			if kvi == nil {
				return nil, nil, nil
			}
		}
		pk = key
		return key, nil, it
	}
	return it, nil
}

// Iterate over all of the values in the tree. See DoIterate() for usage
// details
func (self *BpTree) DoValues(do func([]byte) error) error {
	return doItemIter(
		func() (Iterator, error) { return self.Values() },
		do,
	)
}

// Iterate over all of the values in the tree. See Iterate() for usage
// details
func (self *BpTree) Values() (it Iterator, err error) {
	kvi, err := self.Iterate()
	if err != nil {
		return nil, err
	}
	it = func() (value []byte, err error, _it Iterator) {
		_, value, err, kvi = kvi()
		if err != nil {
			return nil, err, nil
		}
		if kvi == nil {
			return nil, nil, nil
		}
		return value, nil, it
	}
	return it, nil
}

// Iterate over all of the key/values pairs with the given key. See
// DoIterate() for usage details.
func (self *BpTree) DoFind(key []byte, do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Find(key) },
		do,
	)
}

// Iterate over all of the key/values pairs with the given key. See
// Iterate() for usage details.
func (self *BpTree) Find(key []byte) (kvi KVIterator, err error) {
	return self.Range(key, key)
}

// How many key/value pairs are there with the given key.
func (self *BpTree) Count(key []byte) (count int, err error) {
	err = self.DoFind(key, func(k, v []byte) error {
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Iterate over all of the key/values pairs between [from, to]
// inclusive. See DoIterate() for usage details.
func (self *BpTree) DoRange(from, to []byte, do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Range(from, to) },
		do,
	)
}

// Iterate over all of the key/values pairs between [from, to]
// inclusive. See Iterate() for usage details.
func (self *BpTree) Range(from, to []byte) (kvi KVIterator, err error) {
	var bi bpt_iterator
	if bytes.Compare(from, to) <= 0 {
		bi, err = self.forward(from, to)
	} else {
		bi, err = self.forward(to, from)
	}
	if err != nil {
		return nil, err
	}
	kvi = func() (key, value []byte, e error, it KVIterator) {
		var a uint64
		var i int
		a, i, err, bi = bi()
		if err != nil {
			return nil, nil, err, nil
		}
		if bi == nil {
			return nil, nil, nil, nil
		}
		err = self.doKV(a, i, func(k, v []byte) error {
			key = make([]byte, len(k))
			copy(key, k)
			value = make([]byte, len(v))
			copy(value, v)
			return nil
		})
		if err != nil {
			return nil, nil, err, nil
		}
		return key, value, nil, kvi
	}
	return kvi, nil
}

/* returns the key at the address and index or an error
 */
func (self *BpTree) keyAt(a uint64, i int) (key []byte, err error) {
	key = make([]byte, self.meta.keySize)
	err = self.do(
		a,
		func(n *internal) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("out of range")
			}
			copy(key, n.key(i))
			return nil
		},
		func(n *leaf) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("out of range")
			}
			copy(key, n.key(i))
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return key, nil
}

/* returns the (addr, idx) of the leaf block and the index of the key in
 * the block which has a key greater or equal to the search key.
 */
func (self *BpTree) getStart(key []byte) (a uint64, i int, err error) {
	return self._getStart(self.meta.root, key)
}

func (self *BpTree) _getStart(n uint64, key []byte) (a uint64, i int, err error) {
	var flags consts.Flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = consts.Flag(bytes[0])
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if flags&consts.INTERNAL != 0 {
		return self.internalGetStart(n, key)
	} else if flags&consts.LEAF != 0 {
		return self.leafGetStart(n, key)
	} else {
		return 0, 0, errors.Errorf("Unknown block type")
	}
}

func (self *BpTree) internalGetStart(n uint64, key []byte) (a uint64, i int, err error) {
	var kid uint64
	err = self.doInternal(n, func(n *internal) error {
		if key == nil {
			kid = *n.ptr(0)
			return nil
		}
		i, has, err := find(self.varchar, n, key)
		if err != nil {
			return err
		}
		if !has && i > 0 {
			// if it doesn't have it and the index > 0 then we have the
			// next block so we have to subtract one from the index.
			i--
		}
		kid = *n.ptr(i)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return self._getStart(kid, key)
}

func (self *BpTree) leafGetStart(n uint64, key []byte) (a uint64, i int, err error) {
	if key == nil {
		return n, 0, nil
	}
	var next uint64 = 0
	err = self.doLeaf(n, func(n *leaf) (err error) {
		var has bool
		i, has, err = find(self.varchar, n, key)
		if err != nil {
			return err
		}
		if i >= int(n.meta.keyCount) && i > 0 {
			i = int(n.meta.keyCount) - 1
		}
		if !has && n.meta.next != 0 && bytes.Compare(n.key(i), key) < 0 {
			next = n.meta.next
			return nil
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if next != 0 {
		return self.leafGetStart(next, key)
	}
	return n, i, nil
}

func (self *BpTree) forward(from, to []byte) (bi bpt_iterator, err error) {
	a, i, err := self.getStart(from)
	if err != nil {
		return nil, err
	}
	return self.forwardFrom(a, i, to)
}

func (self *BpTree) forwardFrom(a uint64, i int, to []byte) (bi bpt_iterator, err error) {
	i--
	bi = func() (uint64, int, error, bpt_iterator) {
		var err error
		var end bool
		a, i, end, err = self.nextLoc(a, i)
		if err != nil {
			return 0, 0, err, nil
		}
		if end {
			return 0, 0, nil, nil
		}
		if to == nil {
			return a, i, nil, bi
		}
		var less bool = false
		err = self.doLeaf(a, func(n *leaf) error {
			less = bytes.Compare(to, n.key(i)) < 0
			return nil
		})
		if err != nil {
			return 0, 0, err, nil
		}
		if less {
			return 0, 0, nil, nil
		}
		return a, i, nil, bi
	}
	return bi, nil
}

func (self *BpTree) nextLoc(a uint64, i int) (uint64, int, bool, error) {
	j := i + 1
	nextBlk := func(a uint64, j int) (uint64, int, bool, error) {
		changed := false
		err := self.doLeaf(a, func(n *leaf) error {
			if j >= int(n.meta.keyCount) && n.meta.next != 0 {
				a = n.meta.next
				j = 0
				changed = true
			}
			return nil
		})
		if err != nil {
			return 0, 0, false, err
		}
		return a, j, changed, nil
	}
	var changed bool = true
	var err error = nil
	for changed {
		a, j, changed, err = nextBlk(a, j)
		if err != nil {
			return 0, 0, false, err
		}
	}
	var end bool = false
	err = self.doLeaf(a, func(n *leaf) error {
		if j >= int(n.meta.keyCount) {
			end = true
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, err
	}
	return a, j, end, nil
}

func (self *BpTree) prevLoc(a uint64, i int) (uint64, int, bool, error) {
	j := i - 1
	prevBlk := func(a uint64, j int) (uint64, int, bool, error) {
		changed := false
		err := self.doLeaf(a, func(n *leaf) error {
			if j < 0 && n.meta.prev != 0 {
				a = n.meta.prev
				changed = true
				return self.doLeaf(a, func(m *leaf) error {
					j = int(m.meta.keyCount) - 1
					return nil
				})
			}
			return nil
		})
		if err != nil {
			return 0, 0, false, err
		}
		return a, j, changed, nil
	}
	var changed bool = true
	var err error = nil
	for changed {
		a, j, changed, err = prevBlk(a, j)
		if err != nil {
			return 0, 0, false, err
		}
	}
	var end bool = false
	err = self.doLeaf(a, func(n *leaf) error {
		if j < 0 {
			end = true
		}
		return nil
	})
	if err != nil {
		return 0, 0, false, err
	}
	return a, j, end, nil
}
