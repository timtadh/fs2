package bptree

import "testing"

import (
	"bytes"
	"fmt"
)

import (
	"github.com/timtadh/fs2/consts"
)

func (t *T) assert_value(expect []byte) func(value []byte, err error) {
	return func(value []byte, err error) {
		t.assert_nil(err)
		t.assert(fmt.Sprintf("values were not equal %v != %v", expect, value), bytes.Equal(expect, value))
	}
}

func (t *T) assert_notValue(expect []byte) func(value []byte, err error) {
	return func(value []byte, err error) {
		t.assert_nil(err)
		t.assert(fmt.Sprintf("values should not have been equal %v == %v", expect, value), !bytes.Equal(expect, value))
	}
}

func TestBalanceInternal(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	for TEST := 0; TEST < TESTS*10; TEST++ {
		SIZE := 1027 + TEST*16
		if SIZE > consts.BLOCKSIZE {
			SIZE = consts.BLOCKSIZE
		}
		n, err := newInternal(0, make([]byte, SIZE), 8)
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
			t.assert_nil(n.putKP(bpt.varchar, kp.key, kp.ptr))
			t.assert("could not find key in internal", n._has(bpt.varchar, kp.key))
			t.assert_ptr(kp.ptr)(n.findPtr(bpt.varchar, kp.key))
		}
		for _, kp := range kps {
			t.assert("could not find key in internal", n._has(bpt.varchar, kp.key))
			t.assert_ptr(kp.ptr)(n.findPtr(bpt.varchar, kp.key))
		}
		b, err := newInternal(0, make([]byte, SIZE), 8)
		t.assert_nil(err)
		t.assert_nil(n.balance(bpt.varchar, b))
		for _, kp := range kps {
			t.assert("could not find key in internal", n._has(bpt.varchar, kp.key) || b._has(bpt.varchar, kp.key))
			if n._has(bpt.varchar, kp.key) {
				t.assert_ptr(kp.ptr)(n.findPtr(bpt.varchar, kp.key))
			} else {
				t.assert_ptr(kp.ptr)(b.findPtr(bpt.varchar, kp.key))
			}
		}
		for i := 0; i < n.keyCount(); i++ {
			t.assert_nil(n.doKeyAt(bpt.varchar, i, func(n_key_i []byte) error {
				return b.doKeyAt(bpt.varchar, 0, func(b_key_0 []byte) error {
					t.assert("key >= to start key in b", bytes.Compare(n_key_i, b_key_0) < 0)
					return nil
				})
			}))
		}
	}
	clean()
}

func TestBalanceLeaf(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	defer clean()
	for TEST := 0; TEST < TESTS; TEST++ {
		SIZE := 1027 + TEST*16
		if SIZE >= consts.BLOCKSIZE {
			SIZE = consts.BLOCKSIZE
		}
		n, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_key(),
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
		b, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		t.assert_nil(n.balance(bpt.varchar, b))
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key) || b._has(bpt.varchar, kv.key))
			if n._has(bpt.varchar, kv.key) {
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
			} else {
				t.assert_value(kv.value)(b.firstValue(bpt.varchar, kv.key))
			}
		}
		for i := 0; i < n.keyCount(); i++ {
			t.assert_nil(n.doKeyAt(bpt.varchar, i, func(n_key_i []byte) error {
				return b.doKeyAt(bpt.varchar, 0, func(b_key_0 []byte) error {
					t.assert("key >= to start key in b", bytes.Compare(n_key_i, b_key_0) < 0)
					return nil
				})
			}))
		}
	}
}

func TestBalancePureLeaf(x *testing.T) {
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
		type KV struct {
			key   []byte
			value []byte
		}
		only_key := t.rand_key()
		make_kv := func() *KV {
			return &KV{
				key:   only_key,
				value: t.rand_value(8),
			}
		}
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := make_kv()
			if !n.fitsAnother() {
				break
			}
			kvs = append(kvs, kv)
			t.assert_nil(n.putKV(bpt.varchar, kv.key, kv.value))
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
		}
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key))
		}
		b, err := newLeaf(0, make([]byte, SIZE), 8, 8)
		t.assert_nil(err)
		t.assert_nil(n.balance(bpt.varchar, b))
		for _, kv := range kvs {
			t.assert("could not find key in leaf", n._has(bpt.varchar, kv.key) || b._has(bpt.varchar, kv.key))
		}
		for i := 0; i < n.keyCount(); i++ {
			if b.meta.keyCount > 0 {
				t.assert_nil(n.doKeyAt(bpt.varchar, i, func(n_key_i []byte) error {
					return b.doKeyAt(bpt.varchar, 0, func(b_key_0 []byte) error {
						t.assert("key >= to start key in b", bytes.Compare(n_key_i, b_key_0) < 0)
						return nil
					})
				}))
			}
		}
	}
}
