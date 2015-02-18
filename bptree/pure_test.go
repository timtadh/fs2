package bptree

import "testing"

import (
//	"bytes"
	"math/rand"
)

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
		t.assert_has(bpt)(kv.key)
		for i := 0; i < rand.Intn(500) + 1; i++ {
			kv2 := &KV{
				key: kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Put(kv2.key, kv2.value))
			t.assert_has(bpt)(kv2.key)
			//t.assert_hasValue(bpt)(kv2.key, kv2.value)
		}
	}
	for _, key := range keys {
		t.assert_nil(bpt.Remove(key, func(b []byte) bool {
			return true
		}))
	}
	for _, key := range keys {
		t.assert_notHas(bpt)(key)
	}
	clean()
}
