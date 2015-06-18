package bptree

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
)

func (self *BpTree) newInternal() (a uint64, err error) {
	return self.new(func(bytes []byte) error {
		_, err := newInternal(self.meta.flags, bytes, self.meta.keySize)
		return err
	})
}

func (self *BpTree) newLeaf() (a uint64, err error) {
	return self.new(func(bytes []byte) error {
		_, err := newLeaf(self.meta.flags, bytes, self.meta.keySize, self.meta.valSize)
		return err
	})
}

func (self *BpTree) new(init func([]byte) error) (uint64, error) {
	a, err := self.bf.Allocate()
	if err != nil {
		return 0, err
	}
	err = self.bf.Do(a, 1, func(bytes []byte) error {
		return init(bytes)
	})
	if err != nil {
		return 0, err
	}
	return a, nil
}

func (self *BpTree) doInternal(a uint64, do func(*internal) error) error {
	return self.do(
		a,
		do,
		func(n *leaf) error {
			return errors.Errorf("Unexpected leaf node")
		},
	)
}

func (self *BpTree) doLeaf(a uint64, do func(*leaf) error) error {
	return self.do(
		a,
		func(n *internal) error {
			return errors.Errorf("Unexpected internal node")
		},
		do,
	)
}

/* provides a do context for the kv at the given address/idx
 */
func (self *BpTree) doKV(a uint64, i int, do func(key, value []byte) error) (err error) {
	return self.doLeaf(a, func(n *leaf) error {
		if i >= int(n.meta.keyCount) {
			return errors.Errorf("Index out of range")
		}
		return n.doKeyAt(self.varchar, i, func(key []byte) error {
			return n.doValueAt(self.varchar, i, func(value []byte) error {
				return do(key, value)
			})
		})
	})
}

func (self *BpTree) doKey(a uint64, i int, do func(key []byte) error) (err error) {
	return self.do(
		a,
		func(n *internal) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("Index out of range")
			}
			return n.doKeyAt(self.varchar, i, func(key []byte) error {
				return do(key)
			})
		},
		func(n *leaf) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("Index out of range")
			}
			return n.doKeyAt(self.varchar, i, func(key []byte) error {
				return do(key)
			})
		},
	)
}

func (self *BpTree) do(
	a uint64,
	internalDo func(*internal) error,
	leafDo func(*leaf) error,
) error {
	return self.bf.Do(a, 1, func(bytes []byte) error {
		flags := consts.AsFlag(bytes)
		if flags&consts.INTERNAL != 0 {
			return internalDo(asInternal(bytes))
		} else if flags&consts.LEAF != 0 {
			return leafDo(asLeaf(bytes))
		} else {
			return errors.Errorf("Unknown block type")
		}
	})
}
