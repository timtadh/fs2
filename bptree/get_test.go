package bptree

import "testing"

import (
	"bytes"
	"fmt"
	"sort"
)

func TestIterateEmpty(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	t.assert_nil(bpt.DoIterate(func(k, v []byte) error {
		return fmt.Errorf("found something in empty tree %v %v", k, v)
	}))
	clean()
}

func TestFindEmpty(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	t.assert_nil(bpt.DoFind([]byte("wizard"), func(k, v []byte) error {
		return fmt.Errorf("found something in empty tree %v %v", k, v)
	}))
	clean()
}

func TestRangeBackwardEmpty(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	t.assert_nil(bpt.DoRange([]byte("wizard"), []byte("apple"), func(k, v []byte) error {
		return fmt.Errorf("found something in empty tree %v %v", k, v)
	}))
	clean()
}

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

func TestIterateBackward(x *testing.T) {
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
			i := len(kvs) - 1
			t.assert_nil(bpt.DoBackward(
				func(key, value []byte) error {
					t.assert("key should equals kvs[i].key", bytes.Equal(key, kvs[i].key))
					t.assert("values should equals kvs[i].value", bytes.Equal(value, kvs[i].value))
					i--
					return nil
				}))
			t.assert(fmt.Sprintf("i, %v == -1", i), i == -1)
		}
		clean()
	}
}

func TestFind(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 190
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*5)
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
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		sort.Sort(kvs)
		for _, kv := range kvs {
			t.assert_nil(bpt.DoFind(kv.key, func(k, v []byte) error {
				t.assert(fmt.Sprintf("kv.key '%v' == '%v' k", kv.key, k), bytes.Equal(kv.key, k))
				t.assert(fmt.Sprintf("kv.value '%v' == '%v' v", kv.value, v), bytes.Equal(kv.value, v))
				return nil
			}))
		}
		clean()
	}
}

func TestFindSequence(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 190
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*5)
		for i := 0; i < cap(kvs); i++ {
			k := uint64(cap(kvs) - i + 1)
			kv := &KV{
				key:   t.bkey(&k),
				value: t.rand_key(),
			}
			kvs = append(kvs, kv)
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		sort.Sort(kvs)
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		sort.Sort(kvs)
		for _, kv := range kvs {
			t.assert_nil(bpt.DoFind(kv.key, func(k, v []byte) error {
				t.assert(fmt.Sprintf("kv.key '%v' == '%v' k", kv.key, k), bytes.Equal(kv.key, k))
				t.assert(fmt.Sprintf("kv.value '%v' == '%v' v", kv.value, v), bytes.Equal(kv.value, v))
				return nil
			}))
		}
		clean()
	}
}

func TestFindPrefixes(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	kvs := KVS{
		{[]byte{1}, t.rand_key()},
		{[]byte{1, 1}, t.rand_key()},
		{[]byte{1, 1, 1}, t.rand_key()},
	}
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	sort.Sort(kvs)
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	sort.Sort(kvs)
	for _, kv := range kvs {
		t.assert_nil(bpt.DoFind(kv.key, func(k, v []byte) error {
			t.assert(fmt.Sprintf("kv.key '%v' == '%v' k", kv.key, k), bytes.Equal(kv.key, k))
			t.assert(fmt.Sprintf("kv.value '%v' == '%v' v", kv.value, v), bytes.Equal(kv.value, v))
			return nil
		}))
	}
	clean()
}

func TestFindPrefixPartial(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	kvs := KVS{
		{[]byte{0, 1, 0, 3, 0, 5, 0, 1, 0, 2, 0, 3}, t.rand_key()},
		{[]byte{0, 2, 0, 3, 0, 5, 0, 7, 0, 1, 0, 2, 0, 3}, t.rand_key()},
		{[]byte{0, 3, 0, 2, 0, 5, 0, 7, 0, 9, 0, 1, 0, 2}, t.rand_key()},
	}
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	sort.Sort(kvs)
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	for _, kv := range kvs {
		t.assert_nil(bpt.Add(kv.key, kv.value))
	}
	sort.Sort(kvs)
	for _, kv := range kvs {
		t.assert_nil(bpt.DoFind(kv.key, func(k, v []byte) error {
			t.assert(fmt.Sprintf("kv.key '%v' == '%v' k", kv.key, k), bytes.Equal(kv.key, k))
			t.assert(fmt.Sprintf("kv.value '%v' == '%v' v", kv.value, v), bytes.Equal(kv.value, v))
			return nil
		}))
	}
	clean()
}

func TestFindRegress(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	t.assert_nil(bpt.Add([]byte{0, 1, 0, 2, 0, 0, 0, 0}, t.rand_key()))
	key := []byte{0, 1, 0, 3, 0, 0, 0, 0}
	t.assert_nil(bpt.DoFind(key, func(k, v []byte) error {
		t.assert("found key when it wasn't in tree", false)
		return nil
	}))
	clean()
}

func TestRangeRegress(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	t.assert_nil(bpt.Add([]byte{0, 1, 0, 2, 0, 0, 0, 0}, t.rand_key()))
	from := []byte{0, 1, 0, 1, 0, 0, 0, 0}
	to := []byte{0, 1, 0, 0, 0, 0, 0, 0}
	t.assert_nil(bpt.DoRange(from, to, func(k, v []byte) error {
		t.assert("found key when it wasn't in tree", false)
		return nil
	}))
	clean()
}
