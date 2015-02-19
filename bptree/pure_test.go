package bptree

import "testing"

import (
	"bytes"
	"math/rand"
)

func (t *T) assert_hasKV(bpt *BpTree) func(key, value []byte) {
	return func(key, value []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.hasKV(key, value)
		t.assert_nil(err)
		t.assert("should have found kv", has)
	}
}

func (t *T) assert_notHasKV(bpt *BpTree) func(key, value []byte) {
	return func(key, value []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.hasKV(key, value)
		t.assert_nil(err)
		t.assert("should not have found kv", !has)
	}
}

func TestPutRemovePuresRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	keys := make([][]byte, 0, 500)
	kvs := make([]*KV, 0, 500)
	for i := 0; i < 250; i++ {
		kv := &KV{
			key: t.rand_key(),
			value: t.rand_value(24),
		}
		keys = append(keys, kv.key)
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Put(kv.key, kv.value))
		for i := 0; i < rand.Intn(500) + 1; i++ {
			kv2 := &KV{
				key: kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Put(kv2.key, kv2.value))
		}
	}
	for _, kv := range kvs {
		t.assert_hasKV(bpt)(kv.key, kv.value)
	}
	for _, kv := range kvs {
		t.assert_nil(bpt.Remove(kv.key, func(v []byte) bool {
			return bytes.Equal(kv.value, v)
		}))
	}
	for _, key := range keys {
		t.assert_notHas(bpt)(key)
	}
	clean()
}
