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
			key:   t.rand_key(),
			value: t.rand_value(24),
		}
		keys = append(keys, kv.key)
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Add(kv.key, kv.value))
		for i := 0; i < rand.Intn(50)+1; i++ {
			kv2 := &KV{
				key:   kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Add(kv2.key, kv2.value))
		}
	}
	for _, kv := range kvs {
		t.assert_hasKV(bpt)(kv.key, kv.value)
	}
	for i, kv := range kvs {
		found := false
		t.assert_nil(bpt.Remove(kv.key, func(v []byte) bool {
			if found {
				return false
			}
			if bytes.Equal(kv.value, v) {
				found = true
				return true
			}
			return false
		}))
		t.assert("bpt.Size() == len(kvs) - (i + 1)", bpt.Size() == len(kvs) - (i + 1))
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
			key:   t.rand_key(),
			value: t.rand_value(24),
		}
		keys = append(keys, kv.key)
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Add(kv.key, kv.value))
		dups := rand.Intn(500) + 250
		for i := 0; i < dups; i++ {
			// t.Log(i+2)
			kv2 := &KV{
				key:   kv.key,
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv2)
			t.assert_nil(bpt.Add(kv2.key, kv2.value))
		}
	}
	for _, kv := range kvs {
		t.assert_hasKV(bpt)(kv.key, kv.value)
	}
	t.assert("bpt.Size() == len(kvs)", bpt.Size() == len(kvs))
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

func (t *T) setupAlmostPureSplit(small_key []byte) (*BpTree, func(), []byte) {
	bpt, clean := t.bpt()
	small := &KV{
		key: small_key,
		value: t.rand_bytes(4),
	}
	t.assert_nil(bpt.Add(small.key, small.value))
	big_key := t.rand_key()
	a := bpt.meta.root
	t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
		for {
			kv := &KV{
				key: big_key,
				value: t.rand_bytes(120),
			}
			if !n.fits(kv.value) {
				break
			}
			t.assert_nil(bpt.Add(kv.key, kv.value))
			t.assert("a == root", a == bpt.meta.root)
		}
		t.assert("a == root", a == bpt.meta.root)
		return nil
	}))
	return bpt, clean, big_key
}

func TestAlmostPureSplit(x *testing.T) {
	t := (*T)(x)
	// small before, insert < small < big
	{
		small := []byte{0, 0, 0, 0, 0, 0, 0, 1}
		insert := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small before, small = insert < big
	{
		small := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		insert := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small before, small < insert < big
	{
		small := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		insert := []byte{0, 0, 0, 0, 0, 0, 0, 1}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small before, small < insert == big
	{
		small := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: big_key,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small before, small < big < insert
	{
		small := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		insert := make([]byte, len(big_key))
		copy(insert, big_key)
		insert[0] = big_key[0]+1
		insert[2] = big_key[2]+1
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small after, insert == big < small
	{
		small := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: big_key,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small after, insert < big < small
	{
		small := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		insert := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small after, big < insert < small
	{
		small := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		insert := make([]byte, len(big_key))
		copy(insert, big_key)
		insert[0] = big_key[0]+1
		insert[2] = big_key[2]+1
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small after, big < small == insert
	{
		small := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: small,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(big_key)
		clean()
	}
	// small after, big < small < insert
	{
		small := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xfe}
		insert := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		bpt, clean, big_key := t.setupAlmostPureSplit(small)
		kv := &KV{
			key: insert,
			value: t.rand_bytes(120),
		}
		a := bpt.meta.root
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert("a != root", a != bpt.meta.root)
		t.assert_has(bpt)(small)
		t.assert_has(bpt)(insert)
		t.assert_has(bpt)(big_key)
		clean()
	}
}

