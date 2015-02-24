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
	var flags flag
	err := self.bf.Do(a, 1, func(bytes []byte) error {
		flags = flag(bytes[0])
		return nil
	})
	if err != nil {
		return err
	}
	if flags&iNTERNAL != 0 {
		n, cleanup, err := self.getInternal(a)
		if err != nil {
			return err
		}
		defer cleanup()
		return internalDo(n)
	} else if flags&lEAF != 0 {
		n, cleanup, err := self.getLeaf(a)
		if err != nil {
			return err
		}
		defer cleanup()
		return leafDo(n)
	} else {
		return errors.Errorf("Unknown block type")
	}
}

func (self *BpTree) getLeaf(a uint64) (*leaf, func(), error) {
	self.checkCache()
	if n, has := self.leafCache[a]; has {
		return n, func() {}, nil
	}
	bytes, err := self.bf.Get(a, 1)
	if err != nil {
		return nil, nil, err
	}
	n, err := loadLeaf(bytes)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		self.bf.Release(bytes)
	}
	self.leafCache[a] = n
	return n, cleanup, nil
}

func (self *BpTree) getInternal(a uint64) (*internal, func(), error) {
	self.checkCache()
	if n, has := self.internalCache[a]; has {
		return n, func() {}, nil
	}
	bytes, err := self.bf.Get(a, 1)
	if err != nil {
		return nil, nil, err
	}
	n, err := loadInternal(bytes)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		self.bf.Release(bytes)
	}
	self.internalCache[a] = n
	return n, cleanup, nil
}

func (self *BpTree) checkCache() {
	if self.bf.Valid(self.cacheCert) {
		return
	}
	for _, n := range self.leafCache {
		n.release()
	}
	for _, n := range self.internalCache {
		n.release()
	}
	self.leafCache = make(map[uint64]*leaf)
	self.internalCache = make(map[uint64]*internal)
	self.cacheCert = self.bf.Address()
}
