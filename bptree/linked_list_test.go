package bptree

import "testing"

import (
	"github.com/timtadh/fs2/fmap"
)

const PATH = "/tmp/__bptree_test"

func (t *T) blkfile() (*fmap.BlockFile, func()) {
	// bf, err := fmap.CreateBlockFile(PATH)
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
	if err != nil {
		t.Fatal(err)
	}
	return bf, func() {
		err := bf.Close()
		if err != nil {
			t.Fatal(err)
		}
		// err = bf.Remove()
		// if err != nil {
		// t.Fatal(err)
		// }
	}
}

func (t *T) assert_alc(bf *fmap.BlockFile) uint64 {
	a, err := bf.Allocate()
	t.assert_nil(err)
	return a
}

func (t *T) newLeafIn(bf *fmap.BlockFile, a uint64) {
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		_, err := newLeaf(0, bytes, 8, 8)
		return err
	}))
}

func TestLinkedListPut(x *testing.T) {
	t := (*T)(x)
	bpt, cleanup := t.bpt()
	defer cleanup()
	a := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, a)
	b := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, b)
	c := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, c)

	t.assert_nil(bpt.insertListNode(b, 0, 0))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = a", n.meta.prev == 0)
		t.assert("b.next = c", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(bpt.insertListNode(b, a, 0))
	t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
		t.assert("a.next = b", n.meta.next == b)
		n.meta.next = 0
		return nil
	}))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = a", n.meta.prev == a)
		t.assert("b.next = c", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(bpt.insertListNode(b, 0, c))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = a", n.meta.prev == 0)
		t.assert("b.next = c", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(c, func(n *leaf) error {
		t.assert("c.prev = b", n.meta.prev == b)
		n.meta.next = 0
		return nil
	}))

	t.assert_nil(bpt.insertListNode(b, a, c))
	t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
		t.assert("a.next = b", n.meta.next == b)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = a", n.meta.prev == a)
		t.assert("b.next = c", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(c, func(n *leaf) error {
		t.assert("c.prev = b", n.meta.prev == b)
		return nil
	}))
}

func TestLinkedListDel(x *testing.T) {
	t := (*T)(x)
	bpt, cleanup := t.bpt()
	defer cleanup()
	a := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, a)
	b := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, b)
	c := t.assert_alc(bpt.bf)
	t.newLeafIn(bpt.bf, c)

	t.assert_nil(bpt.insertListNode(b, a, 0))
	t.assert_nil(bpt.delListNode(b))
	t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
		t.assert("a.next = 0", n.meta.next == 0)
		n.meta.next = 0
		return nil
	}))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(bpt.insertListNode(b, 0, c))
	t.assert_nil(bpt.delListNode(b))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(c, func(n *leaf) error {
		t.assert("b.prev = 0", n.meta.prev == 0)
		n.meta.next = 0
		return nil
	}))

	t.assert_nil(bpt.insertListNode(b, a, c))
	t.assert_nil(bpt.delListNode(b))
	t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
		t.assert("a.next = 0", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(b, func(n *leaf) error {
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))
	t.assert_nil(bpt.doLeaf(c, func(n *leaf) error {
		t.assert("c.prev = 0", n.meta.prev == a)
		return nil
	}))
}
