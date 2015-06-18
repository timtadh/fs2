package bptree

import (
	"github.com/timtadh/fs2/errors"
)

func (self *BpTree) firstKey(a uint64, do func(key []byte) error) error {
	return self.do(
		a,
		func(n *internal) error {
			if int(n.meta.keyCount) == 0 {
				return errors.Errorf("Block was empty")
			}
			return do(n.key(0))
		},
		func(n *leaf) error {
			if int(n.meta.keyCount) == 0 {
				return errors.Errorf("Block was empty")
			}
			return do(n.key(0))
		},
	)
}
