package bptree

import (
	"bytes"
)

type bpt_iterator func() (a uint64, idx int, err error, bi bpt_iterator)


/* returns the key at the address and index or an error
 */
func (self *BpTree) keyAt(a uint64, i int) (key []byte, err error) {
	key = make([]byte, self.meta.keySize)
	err = self.do(
		a,
		func(n *internal) error {
			if i >= int(n.meta.keyCount) {
				return Errorf("out of range")
			}
			copy(key, n.keys[i])
			return nil
		},
		func(n *leaf) error {
			if i >= int(n.meta.keyCount) {
				return Errorf("out of range")
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
	if flags & INTERNAL != 0 {
		return self.internalGetStart(n, key)
	} else if flags & LEAF != 0 {
		return self.leafGetStart(n, key)
	} else {
		return 0, 0, Errorf("Unknown block type")
	}
}

func (self *BpTree) internalGetStart(n uint64, key []byte) (a uint64, i int, err error) {
	var kid uint64
	err = self.doInternal(n, func(n *internal) error {
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
		var less bool = false
		err = self.doLeaf(a, func(n *leaf) error {
			less = bytes.Compare(to, n.keys[i]) < 0
			return nil
		})
		if err != nil {
			return 0, 0, err, nil
		}
		if end || less {
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

