package bptree

import "testing"

import (
	"bytes"
	"os"
	"runtime/debug"
)

import (
	"github.com/timtadh/fs2/fmap"
)

type B testing.B

func (t *B) blkfile() (*fmap.BlockFile, func()) {
	bf, err := fmap.Anonymous(4096)
	if err != nil {
		t.Fatal(err)
	}
	return bf, func() {
		err := bf.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (t *B) bpt() (*BpTree, func()) {
	bf, bf_clean := t.blkfile()
	bpt, err := New(bf, 8, -1)
	if err != nil {
		t.Fatal(err)
	}
	return bpt, bf_clean
}

func (t *B) assert(msg string, oks ...bool) {
	for _, ok := range oks {
		if !ok {
			t.Log("\n" + string(debug.Stack()))
			t.Error(msg)
			t.Fatal("assert failed")
		}
	}
}

func (t *B) assert_nil(errors ...error) {
	for _, err := range errors {
		if err != nil {
			t.Log("\n" + string(debug.Stack()))
			t.Fatal(err)
		}
	}
}

func (t *B) rand_bytes(length int) []byte {
	if urandom, err := os.Open("/dev/urandom"); err != nil {
		t.Fatal(err)
	} else {
		slice := make([]byte, length)
		if _, err := urandom.Read(slice); err != nil {
			t.Fatal(err)
		}
		urandom.Close()
		return slice
	}
	panic("unreachable")
}

func (t *B) rand_key() []byte {
	return t.rand_bytes(8)
}

func (t *B) rand_value(length int) []byte {
	return t.rand_bytes(length)
}

func (t *B) assert_has(bpt *BpTree) func(key []byte) {
	return func(key []byte) {
		// var err error = nil
		// var has bool = true
		has, err := bpt.Has(key)
		t.assert_nil(err)
		t.assert("should have found key", has)
	}
}

func BenchmarkBpTreeAddHasRemove(x *testing.B) {
	LEAF_CAP := 190
	t := (*B)(x)
	x.StopTimer()
	x.ResetTimer()
	for TEST := 0; TEST < t.N; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*2)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv)
		}
		{
			x.StartTimer()
			for _, kv := range kvs {
				t.assert_nil(bpt.Add(kv.key, kv.value))
			}
			for _, kv := range kvs {
				t.assert_has(bpt)(kv.key)
			}
			for _, kv := range kvs {
				t.assert_nil(bpt.Remove(kv.key, func(b []byte) bool {
					return bytes.Equal(b, kv.value)
				}))
			}
			x.StopTimer()
		}
		clean()
	}
}

func BenchmarkBpTreeAddHas(x *testing.B) {
	LEAF_CAP := 190
	t := (*B)(x)
	x.StopTimer()
	x.ResetTimer()
	for TEST := 0; TEST < t.N; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*2)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_value(8),
			}
			kvs = append(kvs, kv)
		}
		{
			x.StartTimer()
			for _, kv := range kvs {
				t.assert_nil(bpt.Add(kv.key, kv.value))
			}
			for _, kv := range kvs {
				t.assert_has(bpt)(kv.key)
			}
			x.StopTimer()
		}
		clean()
	}
}

func BenchmarkBpTreeAdd(x *testing.B) {
	LEAF_CAP := 190
	t := (*B)(x)
	x.StopTimer()
	x.ResetTimer()
	for TEST := 0; TEST < t.N; TEST++ {
		bpt, clean := t.bpt()
		kvs := make(KVS, 0, LEAF_CAP*2)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_value(8),
			}
			kvs = append(kvs, kv)
		}
		{
			x.StartTimer()
			for _, kv := range kvs {
				t.assert_nil(bpt.Add(kv.key, kv.value))
			}
			x.StopTimer()
		}
		clean()
	}
}
