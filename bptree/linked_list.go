package bptree

import (
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
)

func insertListNode(bf *fmap.BlockFile, node, prev, next uint64) error {
	if node == 0 {
		return errors.Errorf("0 offset for n")
	}
	return bf.Do(node, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		if prev == 0 && next == 0 {
			n.meta.next = 0
			n.meta.prev = 0
			return nil
		} else if next == 0 {
			return bf.Do(prev, 1, func(bytes []byte) error {
				pn, err := loadLeaf(bytes)
				if err != nil {
					return err
				}
				n.meta.next = 0
				n.meta.prev = prev
				pn.meta.next = node
				return nil
			})
		} else if prev == 0 {
			return bf.Do(next, 1, func(bytes []byte) error {
				nn, err := loadLeaf(bytes)
				if err != nil {
					return err
				}
				n.meta.next = next
				n.meta.prev = 0
				nn.meta.prev = node
				return nil
			})
		} else {
			return bf.Do(prev, 1, func(bytes []byte) error {
				pn, err := loadLeaf(bytes)
				if err != nil {
					return err
				}
				return bf.Do(next, 1, func(bytes []byte) error {
					nn, err := loadLeaf(bytes)
					if err != nil {
						return err
					}
					n.meta.next = next
					n.meta.prev = prev
					pn.meta.next = node
					nn.meta.prev = node
					return nil
				})
			})
		}
	})
}

func delListNode(bf *fmap.BlockFile, node uint64) error {
	if node == 0 {
		return errors.Errorf("0 offset for n")
	}
	return bf.Do(node, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		if n.meta.prev != 0 {
			err = bf.Do(n.meta.prev, 1, func(bytes []byte) error {
				pn, err := loadLeaf(bytes)
				if err != nil {
					return err
				}
				pn.meta.next = n.meta.next
				return nil
			})
			if err != nil {
				return err
			}
		}
		if n.meta.next != 0 {
			err = bf.Do(n.meta.next, 1, func(bytes []byte) error {
				nn, err := loadLeaf(bytes)
				if err != nil {
					return err
				}
				nn.meta.prev = n.meta.prev
				return nil
			})
			if err != nil {
				return err
			}
		}
		n.meta.prev = 0
		n.meta.next = 0
		return nil
	})
}
