package bptree

import "testing"

import (
	"bytes"
)

func TestLeafRemove(x *testing.T) {
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
			t.assert_nil(n.putKV(SMALL_VALUE, kv.key, kv.value))
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.first_value(bpt.bf, kv.key))
				return nil
			}))
		}
		for _, kv := range kvs {
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.first_value(bpt.bf, kv.key))
				return nil
			}))
		}
		for idx, kv := range kvs {
			t.assert_nil(bpt.Remove(kv.key, func(value []byte) bool {
				return bytes.Equal(kv.value, value)
			}))
			if idx + 1 == len(kvs) {
				break
			}
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				if t.key(kv.key) == t.key(k) {
					t.assert_notValue(kv.value)(n.first_value(bpt.bf, kv.key))
				}
				return nil
			}))
		}
		clean()
	}
}

func TestLeafBigRemove(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 152
	for TEST := 0; TEST < 10; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, LEAF_CAP)
		for i := 0; i < LEAF_CAP; i++ {
			kv := &KV{
				key: t.rand_key(),
				value: t.rand_bigValue(2048, 4096*5),
			}
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.first_value(bpt.bf, kv.key))
				return nil
			}))
		}
		for _, kv := range kvs {
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.first_value(bpt.bf, kv.key))
				return nil
			}))
		}
		clean()
	}
}

