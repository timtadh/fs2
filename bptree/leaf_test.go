package bptree

import "testing"

import (
)

func TestNewLeaf(t *testing.T) {
	n, err := newLeaf(testAlloc(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if n.meta.flags != LEAF {
		t.Error("was not a LEAF node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 5 {
		t.Error("keyCap was not 5")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.valueSizes[i] != 0 {
			t.Error("ptr was not zero")
		}
	}
	
	n.valueSizes[0] = 1
	n.valueSizes[1] = 21
	n.valueSizes[2] = 23
	n.valueSizes[3] = 125
	n.valueSizes[n.meta.keyCap-1] = 0xffff

	valueSizes := []uint16{1, 21, 23, 125, 0xffff}

	if n.meta.flags != LEAF {
		t.Error("was not an leaf node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 5 {
		t.Error("keyCap was not 5")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}

	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.valueSizes[i] != valueSizes[i] {
			t.Error("valueSize was not the correct value")
		}
	}
}

func TestLoadLeaf(t *testing.T) {
	back := func() []byte {
		n, err := newLeaf(testAlloc(), 16)
		if err != nil {
			t.Fatal(err)
		}
		n.valueSizes[0] = 1
		n.valueSizes[1] = 21
		n.valueSizes[2] = 23
		n.valueSizes[3] = 125
		n.valueSizes[n.meta.keyCap-1] = 0xffff
		return n.back
	}()

	n, err := loadLeaf(back)
	if err != nil {
		t.Fatal(err)
	}

	valueSizes := []uint16{1, 21, 23, 125, 0xffff}

	if n.meta.flags != LEAF {
		t.Error("was not an leaf node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 5 {
		t.Error("keyCap was not 5")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}

	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.valueSizes[i] != valueSizes[i] {
			t.Error("ptr was not the correct value")
		}
	}
}

