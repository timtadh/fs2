package bptree

import "testing"

import (
	"bytes"
)

func testAlloc() []byte {
	return make([]byte, 127)
}

func TestNewInternal(t *testing.T) {
	n := newInternal(testAlloc, 16)
	if n.meta.flags != INTERNAL {
		t.Error("was not an internal node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 4 {
		t.Error("keyCap was not 4")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
	zero := []byte{0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0}
	for i := 0; i < int(n.meta.keyCap); i++ {
		if bytes.Compare(n.keys[i], zero) != 0 {
			t.Error("key was not zero")
		}
		if n.ptrs[i] != 0 {
			t.Error("ptr was not zero")
		}
	}
	
	n.keys[0][0] = 1
	n.keys[n.meta.keyCap-1][15] = 0xf
	n.ptrs[0] = 1
	n.ptrs[1] = 21
	n.ptrs[2] = 23
	n.ptrs[3] = 125
	n.ptrs[n.meta.keyCap-1] = 0xffffffffffffffff

	one := []byte{1,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0}
	fifteen := []byte{0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,15}
	ptrs := []uint64{1, 21, 23, 0xffffffffffffffff}

	if n.meta.flags != INTERNAL {
		t.Error("was not an internal node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 4 {
		t.Error("keyCap was not 4")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
	if bytes.Compare(n.keys[0], one) != 0 {
		t.Error("expected key to lead with 1")
	}
	if bytes.Compare(n.keys[n.meta.keyCap-1], fifteen) != 0 {
		t.Error("expected key to end with 15")
	}

	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.ptrs[i] != ptrs[i] {
			t.Error("ptr was not the correct value")
		}
	}
}

func TestLoadInternal(t *testing.T) {
	back := func() []byte {
		n := newInternal(testAlloc, 16)
		n.keys[0][0] = 1
		n.keys[n.meta.keyCap-1][15] = 0xf
		n.ptrs[0] = 1
		n.ptrs[1] = 21
		n.ptrs[2] = 23
		n.ptrs[3] = 125
		n.ptrs[n.meta.keyCap-1] = 0xffffffffffffffff
		return n.back
	}()

	n := loadInternal(back)

	one := []byte{1,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0}
	fifteen := []byte{0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,15}
	ptrs := []uint64{1, 21, 23, 0xffffffffffffffff}

	if n.meta.flags != INTERNAL {
		t.Error("was not an internal node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 4 {
		t.Error("keyCap was not 4")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
	if bytes.Compare(n.keys[0], one) != 0 {
		t.Error("expected key to lead with 1")
	}
	if bytes.Compare(n.keys[n.meta.keyCap-1], fifteen) != 0 {
		t.Error("expected key to end with 15")
	}

	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.ptrs[i] != ptrs[i] {
			t.Error("ptr was not the correct value")
		}
	}
}

