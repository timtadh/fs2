package bptree

import (
	"bytes"
)

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
		func(n *bigLeaf) error {
			if i >= int(n.meta.keyCount) {
				return Errorf("out of range")
			}
			copy(key, n.key)
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
	} else if flags & BIG_LEAF != 0 {
		return self.bigLeafGetStart(n, key)
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

func (self *BpTree) bigLeafGetStart(n uint64, key []byte) (a uint64, i int, err error) {
	return n, 0, nil
}

