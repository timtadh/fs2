package bptree

import ()

import (
	"github.com/timtadh/fs2/errors"
)

func (self *BpTree) insertListNode(node, prev, next uint64) (err error) {
	if node == 0 {
		return errors.Errorf("0 offset for n")
	}
	return self.doLeaf(node, func(n *leaf) (err error) {
		if prev == 0 && next == 0 {
			n.meta.next = 0
			n.meta.prev = 0
			return nil
		} else if next == 0 {
			return self.doLeaf(prev, func(pn *leaf) (err error) {
				n.meta.next = 0
				n.meta.prev = prev
				pn.meta.next = node
				return nil
			})
		} else if prev == 0 {
			return self.doLeaf(next, func(nn *leaf) (err error) {
				n.meta.next = next
				n.meta.prev = 0
				nn.meta.prev = node
				return nil
			})
		} else {
			return self.doLeaf(prev, func(pn *leaf) (err error) {
				return self.doLeaf(next, func(nn *leaf) (err error) {
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

func (self *BpTree) delListNode(node uint64) (err error) {
	if node == 0 {
		return errors.Errorf("0 offset for n")
	}
	return self.doLeaf(node, func(n *leaf) (err error) {
		if n.meta.prev != 0 {
			err = self.doLeaf(n.meta.prev, func(pn *leaf) (err error) {
				pn.meta.next = n.meta.next
				return nil
			})
			if err != nil {
				return err
			}
		}
		if n.meta.next != 0 {
			err = self.doLeaf(n.meta.next, func(nn *leaf) (err error) {
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
