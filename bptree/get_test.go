package bptree

import "testing"

import (
	"bytes"
	"sort"
)

func TestIterate(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 190
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*2)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_key(),
			}
			kvs = append(kvs, kv)
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		sort.Sort(kvs)
		{
			i := 0
			t.assert_nil(bpt.DoIterate(
				func(key, value []byte) error {
					t.assert("key should equals kvs[i].key", bytes.Equal(key, kvs[i].key))
					t.assert("values should equals kvs[i].value", bytes.Equal(value, kvs[i].value))
					i++
					return nil
				}))
			t.assert("i == len(kvs)", i == len(kvs))
		}
		keys := make([][]byte, 0, LEAF_CAP*2)
		for _, kv := range kvs {
			if len(keys) > 0 {
				if bytes.Equal(keys[len(keys)-1], kv.key) {
					continue
				}
			}
			keys = append(keys, kv.key)
		}
		{
			i := 0
			var key []byte
			ki, err := bpt.Keys()
			t.assert_nil(err)
			for key, err, ki = ki(); ki != nil; key, err, ki = ki() {
				t.assert("key should equals keys[i]", bytes.Equal(key, keys[i]))
				i++
			}
			t.assert_nil(err)
			t.assert("i == len(keys)", i == len(keys))
		}
		{
			i := 0
			t.assert_nil(bpt.DoValues(
				func(value []byte) error {
					t.assert("values should equals kvs[i].value", bytes.Equal(value, kvs[i].value))
					i++
					return nil
				}))
			t.assert("i == len(kvs)", i == len(kvs))
		}
		clean()
	}
}
