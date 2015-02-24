package bptree

import (
	"bytes"
)

// Check for the existence of a given key. An error will be returned if
// there was some problem reading the underlying file.
func (self *BpTree) Has(key []byte) (has bool, err error) {
	a, i, err := self.getStart(key)
	if err != nil {
		return false, err
	}
	empty, err := self.empty(a)
	if err != nil {
		return false, err
	}
	if empty {
		return false, nil
	}
	err = self.doKey(a, i, func(akey []byte) error {
		has = bytes.Equal(key, akey)
		return nil
	})
	if err != nil {
		return false, err
	}
	return has, nil
}

func (self *BpTree) hasKV(key, value []byte) (has bool, err error) {
	next, err := self.forward(key, key)
	if err != nil {
		return false, err
	}
	var a uint64
	var i int
	for a, i, err, next = next(); next != nil; a, i, err, next = next() {
		err = self.doKV(a, i, func(k, v []byte) error {
			has = bytes.Equal(key, k) && bytes.Equal(value, v)
			return nil
		})
		if err != nil {
			return false, err
		}
		if has {
			break
		}
	}
	if err != nil {
		return false, err
	}
	return has, nil
}

func (self *BpTree) empty(a uint64) (empty bool, err error) {
	err = self.do(
		a,
		func(n *internal) error {
			empty = (n.meta.keyCount == 0)
			return nil
		},
		func(n *leaf) error {
			empty = (n.meta.keyCount == 0)
			return nil
		},
	)
	if err != nil {
		return false, err
	}
	return empty, nil
}
