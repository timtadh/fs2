package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/errors"
)

type Iterator func() (item []byte, err error, i Iterator)
type KVIterator func() (key, value []byte,  err error, kvi KVIterator)

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

func (self *BpTree) DoIterate(do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Iterate() },
		do,
	)
}

func (self *BpTree) Iterate() (kvi KVIterator, err error) {
	return self.Range(nil, nil)
}

func (self *BpTree) DoKeys(do func([]byte) error) error {
	return doItemIter(
		func() (Iterator, error) { return self.Keys() },
		do,
	)
}

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
		return key, nil, it
	}
	return it, nil
}

func (self *BpTree) DoValues(do func([]byte) error) error {
	return doItemIter(
		func() (Iterator, error) { return self.Values() },
		do,
	)
}

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

func (self *BpTree) DoFind(key []byte, do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Find(key) },
		do,
	)
}

func (self *BpTree) Find(key []byte) (kvi KVIterator, err error) {
	return self.Range(key, key)
}

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

func (self *BpTree) DoRange(from, to []byte, do func(key, value []byte) error) error {
	return doIter(
		func() (KVIterator, error) { return self.Range(from, to) },
		do,
	)
}

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
	kvi = func()(key, value []byte, e error, it KVIterator) {
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
			value = make([]byte, len(v))
			copy(key, k)
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
			copy(key, n.keys[i])
			return nil
		},
		func(n *leaf) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("out of range")
			}
			copy(key, n.keys[i])
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
	var flags flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = flag(bytes[0])
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if flags & iNTERNAL != 0 {
		return self.internalGetStart(n, key)
	} else if flags & lEAF != 0 {
		return self.leafGetStart(n, key)
	} else {
		return 0, 0, errors.Errorf("Unknown block type")
	}
}

func (self *BpTree) internalGetStart(n uint64, key []byte) (a uint64, i int, err error) {
	var kid uint64
	err = self.doInternal(n, func(n *internal) error {
		if key == nil {
			kid = n.ptrs[0]
			return nil
		}
		i, has := find(int(n.meta.keyCount), n.keys, key)
		if !has && i > 0 {
			// if it doesn't have it and the index > 0 then we have the
			// next block so we have to subtract one from the index.
			i--
		}
		kid = n.ptrs[i]
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
	err = self.doLeaf(n, func(n *leaf) error {
		var has bool
		i, has = find(int(n.meta.keyCount), n.keys, key)
		if i >= int(n.meta.keyCount) && i > 0 {
			i = int(n.meta.keyCount) - 1
		}
		if !has && n.meta.next != 0 && bytes.Compare(n.keys[i], key) < 0 {
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
			less = bytes.Compare(to, n.keys[i]) < 0
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

