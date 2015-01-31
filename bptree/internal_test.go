package bptree

import "testing"

import (
	"bytes"
)

func testAlloc() []byte {
	return make([]byte, 127)
}

func (t *T) newInternal() *internal {
	n, err := newInternal(testAlloc(), 8)
	t.assert_nil(err)
	return n
}

func TestPutKP(x *testing.T) {
	t := (*T)(x)
	n := t.newInternal()
	k1 := uint64(7)
	k2 := uint64(3)
	k3 := uint64(12)
	k4 := uint64(8)
	k5 := uint64(5)
	// t.Log(n)
	t.assert_nil(n.putKP(t.bkey(&k1), k1))
	// t.Log(n)
	t.assert("could not find key in leaf", n.Has(t.bkey(&k1)))
	t.assert_nil(n.putKP(t.bkey(&k2), k2))
	// t.Log(n)
	t.assert("could not find key in leaf", n.Has(t.bkey(&k2)))
	t.assert_nil(n.putKP(t.bkey(&k3), k3))
	// t.Log(n)
	t.assert("could not find key in leaf", n.Has(t.bkey(&k3)))
	t.assert_nil(n.putKP(t.bkey(&k4), k4))
	// t.Log(n)
	t.assert("could not find key in leaf", n.Has(t.bkey(&k4)))
	t.assert_nil(n.putKP(t.bkey(&k5), k5))
	// t.Log(n)
	t.assert("could not find key in leaf", n.Has(t.bkey(&k5)))
	t.assert("could not find key in leaf", n.Has(t.bkey(&k1)))
	t.assert("could not find key in leaf", n.Has(t.bkey(&k2)))
	t.assert("could not find key in leaf", n.Has(t.bkey(&k3)))
	t.assert("could not find key in leaf", n.Has(t.bkey(&k4)))
	t.assert("could not find key in leaf", n.Has(t.bkey(&k5)))
}

func TestNewInternal(t *testing.T) {
	n, err := newInternal(testAlloc(), 16)
	if err != nil {
		t.Fatal(err)
	}
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
		n, err := newInternal(testAlloc(), 16)
		if err != nil {
			t.Fatal(err)
		}
		n.keys[0][0] = 1
		n.keys[n.meta.keyCap-1][15] = 0xf
		n.ptrs[0] = 1
		n.ptrs[1] = 21
		n.ptrs[2] = 23
		n.ptrs[3] = 125
		n.ptrs[n.meta.keyCap-1] = 0xffffffffffffffff
		return n.back
	}()

	n, err := loadInternal(back)
	if err != nil {
		t.Fatal(err)
	}

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

