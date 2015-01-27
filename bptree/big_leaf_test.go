package bptree

import "testing"

import (
)

func TestNewBigLeaf(t *testing.T) {
	n, err := newBigLeaf(testAlloc(), 16, 77)
	if err != nil {
		t.Fatal(err)
	}
	if n.meta.flags != BIG_LEAF {
		t.Error("was not a LEAF node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 1 {
		t.Error("keyCap was not 5")
	}
	if n.meta.keyCount != 1 {
		t.Error("keyCount was not 1")
	}
	t.Log(n)
}

func TestLoadBigLeaf(t *testing.T) {
	back := func() []byte {
		n, err := newBigLeaf(testAlloc(), 16, 77)
		if err != nil {
			t.Fatal(err)
		}
		n.key[0] = 1
		n.key[len(n.key)-1] = 15
		n.value[0] = 1
		n.value[len(n.value)-1] = 15
		return n.back
	}()

	n, err := loadBigLeaf(back)
	if err != nil {
		t.Fatal(err)
	}
	if n.meta.flags != BIG_LEAF {
		t.Error("was not a LEAF node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 1 {
		t.Error("keyCap was not 5")
	}
	if n.meta.keyCount != 1 {
		t.Error("keyCount was not 1")
	}
	t.Log(n)
}

