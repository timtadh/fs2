package bptree

import "testing"

import (
	"bytes"
	"fmt"
	"math/rand"
)

func (t *T) assert_hasKV(bpt *BpTree) func(key, value []byte) {
	return func(key, value []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.hasKV(key, value)
		t.assert_nil(err)
		t.assert(fmt.Sprintf("should have found kv, %v, %v", key, value), has)
	}
}

func (t *T) assert_notHasKV(bpt *BpTree) func(key, value []byte) {
	return func(key, value []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.hasKV(key, value)
		t.assert_nil(err)
		t.assert(fmt.Sprintf("should not have found kv, %v, %v", key, value), !has)
	}
}

func TestAddRemovePuresNoSplitRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	keys := make([][]byte, 0, 500)
	kvs := make([]*KV, 0, 500)
	for i := 0; i < 500; i++ {
		kv := &KV{
			key: t.rand_key(),
			value: t.rand_value(24),
		}
		keys = append(keys, kv.key)
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Add(kv.key, kv.value))
		for i := 0; i < rand.Intn(50) + 1; i++ {
			kv2 := &KV{
				key: kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Add(kv2.key, kv2.value))
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

func TestAddRemovePuresSplitRand(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	keys := make([][]byte, 0, 500)
	kvs := make([]*KV, 0, 10000)
	for i := 0; i < 10; i++ {
		kv := &KV{
			key: t.rand_key(),
			value: t.rand_value(24),
		}
		keys = append(keys, kv.key)
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Add(kv.key, kv.value))
		dups := rand.Intn(500) + 250
		for i := 0; i < dups; i++ {
			// t.Log(i+2)
			kv2 := &KV{
				key: kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Add(kv2.key, kv2.value))
		}
	}
	for _, kv := range kvs {
		t.assert_hasKV(bpt)(kv.key, kv.value)
	}
	// note, there is a good chance for inserting dups values for a key
	// therefore, while it would better to check that there is no bugs
	// in individually removing each value. I am going to instead just
	// remove them all at once
	for _, key := range keys {
		t.assert_nil(bpt.Remove(key, func(v []byte) bool {
			return true
		}))
	}
	for _, key := range keys {
		t.assert_notHas(bpt)(key)
	}
	clean()
}

