package bptree

import "testing"

import (
	"fmt"
)

func (t *T) bpt() (*BpTree, func()) {
	bf, bf_clean := t.blkfile()
	bpt, err := New(bf, 8)
	if err != nil {
		t.Fatal(err)
	}
	return bpt, bf_clean
}

type KV struct {
	key []byte
	value []byte
}

func (t *T) make_kv() *KV {
	return &KV{
		key: t.rand_key(),
		value: t.rand_value(24),
	}
}

func TestLeafInsert(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		SIZE := 1027+TEST*16
		bpt, clean := t.bpt()
		n, err := newLeaf(make([]byte, SIZE), 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := t.make_kv()
			if !n.fits(kv.value) {
				break
			}
			kvs = append(kvs, kv)
			t.assert_nil(n.putKV(kv.key, kv.value))
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
		}
		clean()
	}
}

func TestLeafSplit(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, 200)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := t.make_kv()
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
		}
		clean()
	}
}

func TestInsert3Level(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	kvs := make([]*KV, 0, 200000)
	// t.Log(n)
	for i := 0; i < cap(kvs); i++ {
		kv := t.make_kv()
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Put(kv.key, kv.value))
		a, i, err := bpt.getStart(kv.key)
		t.assert_nil(err)
		k, err := bpt.keyAt(a, i)
		t.assert_nil(err)
		k1 := t.key(kv.key)
		k2 := t.key(k)
		t.assert(fmt.Sprintf("wrong key %v == %v", k1, k2), k1 == k2)
	}
	clean()
}

