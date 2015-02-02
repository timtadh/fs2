package bptree

import "testing"

import (
	"github.com/timtadh/fs2/fmap"
)

const PATH = "/tmp/__bptree_test"

func (t *T) blkfile() (*fmap.BlockFile, func()) {
	bf, err := fmap.CreateBlockFile(PATH)
	if err != nil {
		t.Fatal(err)
	}
	return bf, func() {
		err := bf.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = bf.Remove()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (t *T) assert_alc(bf *fmap.BlockFile) (uint64) {
	a, err := bf.Allocate()
	t.assert_nil(err)
	return a
}

func (t *T) newLeafIn(bf *fmap.BlockFile, a uint64) {
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		_, err := newLeaf(bytes, 8)
		return err
	}))
}

func TestLinkedListPut(x *testing.T) {
	t := (*T)(x)
	bf, cleanup := t.blkfile()
	defer cleanup()
	a := t.assert_alc(bf)
	t.newLeafIn(bf, a)
	b := t.assert_alc(bf)
	t.newLeafIn(bf, b)
	c := t.assert_alc(bf)
	t.newLeafIn(bf, c)

	t.assert_nil(insertListNode(bf, b, 0, 0))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = a", n.meta.prev == 0)
		t.assert("b.next = c", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(insertListNode(bf, b, a, 0))
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("a.next = b", n.meta.next == b)
		n.meta.next = 0
		return nil
	}))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = a", n.meta.prev == a)
		t.assert("b.next = c", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(insertListNode(bf, b, 0, c))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = a", n.meta.prev == 0)
		t.assert("b.next = c", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bf.Do(c, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("c.prev = b", n.meta.prev == b)
		n.meta.next = 0
		return nil
	}))

	t.assert_nil(insertListNode(bf, b, a, c))
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("a.next = b", n.meta.next == b)
		return nil
	}))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = a", n.meta.prev == a)
		t.assert("b.next = c", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bf.Do(c, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("c.prev = b", n.meta.prev == b)
		return nil
	}))
}


func TestLinkedListDel(x *testing.T) {
	t := (*T)(x)
	bf, cleanup := t.blkfile()
	defer cleanup()
	a := t.assert_alc(bf)
	t.newLeafIn(bf, a)
	b := t.assert_alc(bf)
	t.newLeafIn(bf, b)
	c := t.assert_alc(bf)
	t.newLeafIn(bf, c)

	t.assert_nil(insertListNode(bf, b, a, 0))
	t.assert_nil(delListNode(bf, b))
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("a.next = 0", n.meta.next == 0)
		n.meta.next = 0
		return nil
	}))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))

	t.assert_nil(insertListNode(bf, b, 0, c))
	t.assert_nil(delListNode(bf, b))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))
	t.assert_nil(bf.Do(c, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = 0", n.meta.prev == 0)
		n.meta.next = 0
		return nil
	}))

	t.assert_nil(insertListNode(bf, b, a, c))
	t.assert_nil(delListNode(bf, b))
	t.assert_nil(bf.Do(a, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("a.next = 0", n.meta.next == c)
		return nil
	}))
	t.assert_nil(bf.Do(b, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("b.prev = 0", n.meta.prev == 0)
		t.assert("b.next = 0", n.meta.next == 0)
		return nil
	}))
	t.assert_nil(bf.Do(c, 1, func(bytes []byte) error {
		n, err := loadLeaf(bytes)
		if err != nil {
			return err
		}
		t.assert("c.prev = 0", n.meta.prev == a)
		return nil
	}))
}





