package bptree

import "testing"

import (
	"bytes"
)

func (t *T) assert_has(bpt *BpTree) func(key []byte) {
	return func(key []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.Has(key)
		t.assert_nil(err)
		t.assert("should have found key", has)
	}
}

func (t *T) assert_notHas(bpt *BpTree) func(key []byte) {
	return func(key []byte) {
		// var err error = nil
		// var has bool = false
		has, err := bpt.Has(key)
		t.assert_nil(err)
		t.assert("should not have found key", !has)
	}
}

func TestLeafRemove(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		SIZE := 1027 + TEST*16
		bpt, clean := t.bpt()
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
			t.assert_nil(bpt.Add(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
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
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
				return nil
			}))
		}
		for idx, kv := range kvs {
			t.assert_nil(bpt.Remove(kv.key, func(value []byte) bool {
				return bytes.Equal(kv.value, value)
			}))
			if idx+1 == len(kvs) {
				break
			}
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				if t.key(kv.key) == t.key(k) {
					t.assert_notValue(kv.value)(n.firstValue(bpt.varchar, kv.key))
				}
				return nil
			}))
		}
		clean()
	}
}

/*
DISABLED
func TestLeafBigRemove(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 152
	for TEST := 0; TEST < 5; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, LEAF_CAP)
		for i := 0; i < LEAF_CAP; i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_bigValue(2048, 4096*5),
			}
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Add(kv.key, kv.value))
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
			if idx+1 == len(kvs) {
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
*/

func TestAddRemoveRand(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, 500)
		for i := 0; i < cap(kvs); i++ {
			kv := t.make_kv()
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Add(kv.key, kv.value))
			t.assert_has(bpt)(kv.key)
		}
		t.assert_nil(bpt.Verify())
		for _, kv := range kvs {
			t.assert_has(bpt)(kv.key)
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Remove(kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			/*
				for _, kv2 := range kvs[:i+1] {
					t.assert_notHas(bpt)(kv2.key)
				}*/
		}
		t.assert_nil(bpt.Verify())
		for _, kv := range kvs {
			t.assert_notHas(bpt)(kv.key)
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Add(kv.key, kv.value))
			t.assert_has(bpt)(kv.key)
		}
		t.assert_nil(bpt.Verify())
		/*
			for _, kv := range kvs {
				t.assert_has(bpt)(kv.key)
			}*/
		for _, kv := range kvs {
			t.assert_nil(bpt.Remove(kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			/*
				for _, kv2 := range kvs {
					if !bytes.Equal(kv.key, kv2.key) {
						t.assert_has(bpt)(kv2.key)
					}
				}*/
			t.assert_notHas(bpt)(kv.key)
			t.assert_nil(bpt.Add(kv.key, kv.value))
		}
		t.assert_nil(bpt.Verify())
		for _, kv := range kvs {
			t.assert_has(bpt)(kv.key)
		}
		for _, kv := range kvs {
			t.assert_nil(bpt.Remove(kv.key, func(b []byte) bool {
				return bytes.Equal(b, kv.value)
			}))
			/*
				for _, kv2 := range kvs[:i+1] {
					t.assert_notHas(bpt)(kv2.key)
				}*/
		}
		t.assert_nil(bpt.Verify())
		for _, kv := range kvs {
			t.assert_notHas(bpt)(kv.key)
		}
		clean()
	}
}
