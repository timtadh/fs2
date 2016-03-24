package bptree

import "testing"

import (
	"bytes"
	// "fmt"
	"os"
	"runtime/debug"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/slice"
)

const TESTS = 50

type T testing.T

func (t *T) Log(msgs ...interface{}) {
	x := (*testing.T)(t)
	// fmt.Println(msgs...)
	x.Log(msgs...)
}

func (t *T) assert(msg string, oks ...bool) {
	for _, ok := range oks {
		if !ok {
			t.Log("\n" + string(debug.Stack()))
			t.Error(msg)
			t.Fatal("assert failed")
		}
	}
}

func (t *T) assert_nil(errors ...error) {
	for _, err := range errors {
		if err != nil {
			t.Log("\n" + string(debug.Stack()))
			t.Fatal(err)
		}
	}
}

func (t *T) rand_bytes(length int) []byte {
	if urandom, err := os.Open("/dev/urandom"); err != nil {
		t.Fatal(err)
	} else {
		slice := make([]byte, length)
		if _, err := urandom.Read(slice); err != nil {
			t.Fatal(err)
		}
		urandom.Close()
		return slice
	}
	panic("unreachable")
}

func (t *T) rand_key() []byte {
	return t.rand_bytes(8)
}

func (t *T) rand_value(size int) []byte {
	return t.rand_bytes(size)
}

func (t *T) rand_varchar(min, max int) []byte {
	bytes := t.rand_bytes(4)
	s := slice.AsSlice(&bytes)
	length := int(*(*uint32)(s.Array))
	length = (length % (max)) + min
	return t.rand_bytes(length)
}

func (t *T) bkey(key *uint64) []byte {
	s := &slice.Slice{
		Array: unsafe.Pointer(key),
		Len:   8,
		Cap:   8,
	}
	return *s.AsBytes()
}

func (t *T) key(bytes []byte) uint64 {
	t.assert("bytes must have length 8", len(bytes) == 8)
	s := slice.AsSlice(&bytes)
	return *(*uint64)(s.Array)
}

func (t *T) newLeaf() *leaf {
	n, err := newLeaf(0, testAlloc(), 8, 8)
	t.assert_nil(err)
	return n
}

func TestPutKVRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	defer clean()
	for TEST := 0; TEST < TESTS; TEST++ {
		SIZE := 1027 + TEST*16
		if SIZE > consts.BLOCKSIZE {
			SIZE = consts.BLOCKSIZE
		}
		n, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_value(8),
			}
			if !n.fitsAnother() {
				break
			}
			kvs = append(kvs, kv)
			// t.Log(n)
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
	}
}

func TestPutDelKVRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	defer clean()
	for TEST := 0; TEST < TESTS*2; TEST++ {
		SIZE := 1027 + TEST*16
		if SIZE > consts.BLOCKSIZE {
			SIZE = consts.BLOCKSIZE
		}
		n, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_value(8),
			}
			if !n.fitsAnother() {
				break
			}
			kvs = append(kvs, kv)
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
		for i, kv := range kvs {
			t.assert_nil(n.delKV(bpt.varchar, kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			for _, kv2 := range kvs[:i+1] {
				t.assert("found key in leaf", !n._has(bpt.varchar, kv2.key))
			}
		}
		for _, kv := range kvs {
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
		}
		for _, kv := range kvs {
			t.assert_nil(n.delKV(bpt.varchar, kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			for _, kv2 := range kvs {
				if !bytes.Equal(kv.key, kv2.key) {
					t.assert("no key in leaf", n._has(bpt.varchar, kv2.key))
				}
			}
			t.assert("found key in leaf", !n._has(bpt.varchar, kv.key))
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
		}
		for i, kv := range kvs {
			t.assert_nil(n.delKV(bpt.varchar, kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			for _, kv2 := range kvs[:i+1] {
				t.assert("found key in leaf", !n._has(bpt.varchar, kv2.key))
			}
		}
	}
}

func TestPutRepKVRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	defer clean()
	for TEST := 0; TEST < TESTS*10; TEST++ {
		KEYS := (TEST % 7) + 1
		SIZE := 1027 + TEST*16
		if SIZE > consts.BLOCKSIZE {
			SIZE = consts.BLOCKSIZE
		}
		keys := make([][]byte, 0, KEYS)
		for i := 0; i < KEYS; i++ {
			keys = append(keys, t.rand_key())
		}
		n, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   keys[i%len(keys)],
				value: t.rand_value(8),
			}
			if !n.fitsAnother() {
				break
			}
			kvs = append(kvs, kv)
			// t.Log(n)
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			has, err := n.hasValue(bpt.varchar, kv.key, kv.value)
			t.assert_nil(err)
			t.assert("could not find value in leaf", has)
		}
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
			has, err := n.hasValue(bpt.varchar, kv.key, kv.value)
			t.assert_nil(err)
			t.assert("could not find value in leaf", has)
		}
	}
}

func TestPutKV(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	defer clean()
	n, err := newLeaf(0, make([]byte, 256), 8, 8)
	t.assert_nil(err)
	k1 := uint64(7)
	v1 := t.rand_bytes(8)
	k2 := uint64(3)
	v2 := t.rand_bytes(8)
	k3 := uint64(12)
	v3 := t.rand_bytes(8)
	k4 := uint64(8)
	v4 := t.rand_bytes(8)
	k5 := uint64(5)
	v5 := t.rand_bytes(8)
	t.assert_nil(n.putKV(bpt.varchar, t.bkey(&k1), v1))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k1)))
	t.assert_value(v1)(n.firstValue(bpt.varchar, t.bkey(&k1)))

	t.assert_nil(n.putKV(bpt.varchar, t.bkey(&k2), v2))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k2)))
	t.assert_value(v2)(n.firstValue(bpt.varchar, t.bkey(&k2)))

	t.assert_nil(n.putKV(bpt.varchar, t.bkey(&k3), v3))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k3)))
	t.assert_value(v3)(n.firstValue(bpt.varchar, t.bkey(&k3)))

	t.assert_nil(n.putKV(bpt.varchar, t.bkey(&k4), v4))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k4)))
	t.assert_value(v4)(n.firstValue(bpt.varchar, t.bkey(&k4)))

	t.assert_nil(n.putKV(bpt.varchar, t.bkey(&k5), v5))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k5)))
	t.assert_value(v5)(n.firstValue(bpt.varchar, t.bkey(&k5)))

	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k1)))
	t.assert_value(v1)(n.firstValue(bpt.varchar, t.bkey(&k1)))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k2)))
	t.assert_value(v2)(n.firstValue(bpt.varchar, t.bkey(&k2)))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k3)))
	t.assert_value(v3)(n.firstValue(bpt.varchar, t.bkey(&k3)))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k4)))
	t.assert_value(v4)(n.firstValue(bpt.varchar, t.bkey(&k4)))
	t.assert("could not find key in leaf", n._has(bpt.varchar, t.bkey(&k5)))
	t.assert_value(v5)(n.firstValue(bpt.varchar, t.bkey(&k5)))
}

func TestNewLeaf(t *testing.T) {
	n, err := newLeaf(0, testAlloc(), 16, 12)
	if err != nil {
		t.Fatal(err)
	}
	if n.meta.flags != consts.LEAF {
		t.Error("was not a LEAF node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.valSize != 12 {
		t.Error("valSize was not 12")
	}
	if n.meta.keyCap != 3 {
		t.Error("keyCap was not 3")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}

	if n.meta.flags != consts.LEAF {
		t.Error("was not an leaf node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.keyCap != 3 {
		t.Error("keyCap was not 3")
	}
	if n.meta.valSize != 12 {
		t.Error("valSize was not 12")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
}

func TestLoadLeaf(t *testing.T) {
	back := func() []byte {
		n, err := newLeaf(0, testAlloc(), 16, 12)
		if err != nil {
			t.Fatal(err)
		}
		s := &slice.Slice{
			Array: unsafe.Pointer(n),
			Len:   consts.BLOCKSIZE,
			Cap:   consts.BLOCKSIZE,
		}
		return *s.AsBytes()
	}()

	n, err := loadLeaf(back)
	if err != nil {
		t.Fatal(err)
	}

	if n.meta.flags != consts.LEAF {
		t.Error("was not an leaf node")
	}
	if n.meta.keySize != 16 {
		t.Error("keySize was not 16")
	}
	if n.meta.valSize != 12 {
		t.Error("valSize was not 12")
	}
	if n.meta.keyCap != 3 {
		t.Error("keyCap was not 3")
	}
	if n.meta.keyCount != 0 {
		t.Error("keyCount was not 0")
	}
}
