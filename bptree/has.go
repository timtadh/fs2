package bptree

import (
	"bytes"
)

func (self *BpTree) Has(key []byte) (has bool, err error) {
	a, i, err := self.getStart(key)
	if err != nil {
		return false, err
	}
	empty, err := self.empty(a)
	if err != nil{
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

