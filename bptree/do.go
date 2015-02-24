package bptree

import (
	"github.com/timtadh/fs2/errors"
)

func (self *BpTree) newInternal() (a uint64, err error) {
	return self.new(func(bytes []byte) error {
		_, err := newInternal(bytes, self.meta.keySize)
		return err
	})
}

func (self *BpTree) newLeaf() (a uint64, err error) {
	return self.new(func(bytes []byte) error {
		_, err := newLeaf(bytes, self.meta.keySize)
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
		return n.doValueAt(self.bf, i, func(value []byte) error {
			return do(n.keys[i], value)
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
			return do(n.keys[i])
		},
		func(n *leaf) error {
			if i >= int(n.meta.keyCount) {
				return errors.Errorf("Index out of range")
			}
			return do(n.keys[i])
		},
	)
}

func (self *BpTree) do(
	a uint64,
	internalDo func(*internal) error,
	leafDo func(*leaf) error,
) error {
	return self.bf.Do(a, 1, func(bytes []byte) error {
		flags := flag(bytes[0])
		if flags & iNTERNAL != 0 {
			n, err := loadInternal(bytes)
			if err != nil {
				return err
			}
			defer n.release()
			return internalDo(n)
		} else if flags & lEAF != 0 {
			n, err := loadLeaf(bytes)
			if err != nil {
				return err
			}
			defer n.release()
			return leafDo(n)
		} else {
			return errors.Errorf("Unknown block type")
		}
	})
}

