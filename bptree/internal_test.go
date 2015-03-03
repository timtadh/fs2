package bptree

import "testing"

import (
	"bytes"
	"fmt"
)

func testAlloc() []byte {
	return make([]byte, 127)
}

func (t *T) newInternal() *internal {
	n, err := newInternal(testAlloc(), 8)
	t.assert_nil(err)
	return n
}

func (t *T) assert_ptr(expect uint64) func(ptr uint64, err error) {
	return func(ptr uint64, err error) {
		t.assert_nil(err)
		t.assert(fmt.Sprintf("ptrs were not equal %d != %d", expect, ptr), expect == ptr)
	}
}

func TestPutDelKPRand(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		n, err := newInternal(make([]byte, 1027+TEST*16), 8)
		t.assert_nil(err)
		kps := make([]*KP, 0, n.meta.keyCap-1)
		for i := 0; i < cap(kps); i++ {
			kp := t.make_kp()
			kps = append(kps, kp)
			t.assert_nil(n.putKP(kp.key, kp.ptr))
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for _, kp := range kps {
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for i, kp := range kps {
			t.assert_nil(n.delKP(kp.key))
			for _, kp2 := range kps[:i+1] {
				t.assert("found key in internal", !n.Has(kp2.key))
			}
		}
		for _, kp := range kps {
			t.assert_nil(n.putKP(kp.key, kp.ptr))
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for i, kp := range kps {
			t.assert_nil(n.delKP(kp.key))
			t.assert("found key in internal", !n.Has(kp.key))
			for j, kp2 := range kps {
				if j != i {
					t.assert("could not find key in internal", n.Has(kp2.key))
				}
			}
			t.assert_nil(n.putKP(kp.key, kp.ptr))
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for _, kp := range kps {
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for _, kp := range kps {
			t.assert_nil(n.delKP(kp.key))
		}
		for _, kp := range kps {
			t.assert("found key in internal", !n.Has(kp.key))
		}
	}
}

func TestPutKPRand(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS*5; TEST++ {
		n, err := newInternal(make([]byte, 1027+TEST*16), 8)
		t.assert_nil(err)
		type KP struct {
			key []byte
			ptr uint64
		}
		make_kp := func() *KP {
			return &KP{
				key: t.rand_key(),
				ptr: t.key(t.rand_key()),
			}
		}
		kps := make([]*KP, 0, n.meta.keyCap-1)
		for i := 0; i < cap(kps); i++ {
			kp := make_kp()
			kps = append(kps, kp)
			t.assert_nil(n.putKP(kp.key, kp.ptr))
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
		for _, kp := range kps {
			t.assert("could not find key in internal", n.Has(kp.key))
			t.assert_ptr(kp.ptr)(n.ptr(kp.key))
		}
	}
}

func TestPutKP(x *testing.T) {
	t := (*T)(x)
	n := t.newInternal()
	k1 := uint64(7)
	k2 := uint64(3)
	k3 := uint64(12)
	k4 := uint64(8)
	k5 := uint64(5)
	t.assert_nil(n.putKP(t.bkey(&k1), k1))
	t.assert("could not find key in internal", n.Has(t.bkey(&k1)))
	t.assert_ptr(k1)(n.ptr(t.bkey(&k1)))

	t.assert_nil(n.putKP(t.bkey(&k2), k2))
	t.assert("could not find key in internal", n.Has(t.bkey(&k2)))
	t.assert_ptr(k2)(n.ptr(t.bkey(&k2)))

	t.assert_nil(n.putKP(t.bkey(&k3), k3))
	t.assert("could not find key in internal", n.Has(t.bkey(&k3)))
	t.assert_ptr(k3)(n.ptr(t.bkey(&k3)))

	t.assert_nil(n.putKP(t.bkey(&k4), k4))
	t.assert("could not find key in internal", n.Has(t.bkey(&k4)))
	t.assert_ptr(k4)(n.ptr(t.bkey(&k4)))

	t.assert_nil(n.putKP(t.bkey(&k5), k5))
	t.assert("could not find key in internal", n.Has(t.bkey(&k5)))
	t.assert_ptr(k5)(n.ptr(t.bkey(&k5)))

	t.assert("could not find key in internal", n.Has(t.bkey(&k1)))
	t.assert("could not find key in internal", n.Has(t.bkey(&k2)))
	t.assert("could not find key in internal", n.Has(t.bkey(&k3)))
	t.assert("could not find key in internal", n.Has(t.bkey(&k4)))
	t.assert("could not find key in internal", n.Has(t.bkey(&k5)))
	t.assert_ptr(k1)(n.ptr(t.bkey(&k1)))
	t.assert_ptr(k2)(n.ptr(t.bkey(&k2)))
	t.assert_ptr(k3)(n.ptr(t.bkey(&k3)))
	t.assert_ptr(k4)(n.ptr(t.bkey(&k4)))
	t.assert_ptr(k5)(n.ptr(t.bkey(&k5)))
}

func TestNewInternal(t *testing.T) {
	n, err := newInternal(testAlloc(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if n.meta.flags != iNTERNAL {
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
	zero := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	for i := 0; i < int(n.meta.keyCap); i++ {
		if bytes.Compare(n.key(i), zero) != 0 {
			t.Error("key was not zero")
		}
		if n.ptrs[i] != 0 {
			t.Error("ptr was not zero")
		}
	}

	n.key(0)[0] = 1
	n.key(int(n.meta.keyCap-1))[15] = 0xf
	n.ptrs[0] = 1
	n.ptrs[1] = 21
	n.ptrs[2] = 23
	n.ptrs[3] = 125
	n.ptrs[n.meta.keyCap-1] = 0xffffffffffffffff

	one := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fifteen := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15}
	ptrs := []uint64{1, 21, 23, 0xffffffffffffffff}

	if n.meta.flags != iNTERNAL {
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
	if bytes.Compare(n.key(0), one) != 0 {
		t.Error("expected key to lead with 1")
	}
	if bytes.Compare(n.key(int(n.meta.keyCap-1)), fifteen) != 0 {
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
		n.key(0)[0] = 1
		n.key(int(n.meta.keyCap-1))[15] = 0xf
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

	one := []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	fifteen := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15}
	ptrs := []uint64{1, 21, 23, 0xffffffffffffffff}

	if n.meta.flags != iNTERNAL {
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
	if bytes.Compare(n.key(0), one) != 0 {
		t.Error("expected key to lead with 1")
	}
	if bytes.Compare(n.key(int(n.meta.keyCap-1)), fifteen) != 0 {
		t.Error("expected key to end with 15")
	}

	for i := 0; i < int(n.meta.keyCap); i++ {
		if n.ptrs[i] != ptrs[i] {
			t.Error("ptr was not the correct value")
		}
	}
}
