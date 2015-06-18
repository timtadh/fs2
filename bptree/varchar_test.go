package bptree

import "testing"

import (
	"bytes"
	"fmt"
)

import (
	"github.com/timtadh/fs2/fmap"
)

type AV struct {
	a     uint64
	value []byte
}

func (t *T) varchar() (*Varchar, func()) {
	bf, clean := t.blkfile()
	a, err := bf.Allocate()
	t.assert_nil(err)
	v, err := NewVarchar(bf, a)
	t.assert_nil(err)
	return v, clean
}

func TestVarcharNew(x *testing.T) {
	t := (*T)(x)
	v, clean := t.varchar()
	defer clean()
	_, err := v.Alloc(128)
	t.assert_nil(err)
}

func TestVarcharOpen(x *testing.T) {
	t := (*T)(x)
	v, clean := t.varchar()
	defer clean()
	a, err := v.Alloc(128)
	r := t.rand_bytes(128)
	t.assert_nil(err)
	t.assert_nil(v.Do(a, func(bytes []byte) error {
		t.assert("len(bytes) == 128", len(bytes) == 128)
		copy(bytes, r)
		return nil
	}))
	v2, err := OpenVarchar(v.bf, v.a)
	t.assert_nil(err)
	t.assert_nil(v2.Do(a, func(data []byte) error {
		t.assert("data == r", bytes.Equal(data, r))
		return nil
	}))
}

func TestVarcharAlloc(x *testing.T) {
	t := (*T)(x)
	v, clean := t.varchar()
	defer clean()
	avs := make([]*AV, 0, TESTS)
	for i := 0; i < TESTS; i++ {
		r := t.rand_varchar(0, fmap.BLOCKSIZE*12)
		a, err := v.Alloc(len(r))
		t.assert_nil(err)
		t.assert_nil(v.Do(a, func(data []byte) error {
			copy(data, r)
			return nil
		}))
		avs = append(avs, &AV{a, r})
	}
	for _, av := range avs {
		t.assert_nil(v.Do(av.a, func(data []byte) error {
			t.assert("data == r", bytes.Equal(data, av.value))
			return nil
		}))
	}
}

func TestVarcharFree(x *testing.T) {
	t := (*T)(x)
	v, clean := t.varchar()
	defer clean()
	avs := make([]*AV, 0, TESTS)
	for i := 0; i < TESTS; i++ {
		r := t.rand_varchar(0, fmap.BLOCKSIZE*12)
		a, err := v.Alloc(len(r))
		t.assert_nil(err)
		t.assert_nil(v.Do(a, func(data []byte) error {
			copy(data, r)
			return nil
		}))
		avs = append(avs, &AV{a, r})
	}
	for _, av := range avs {
		t.assert_nil(v.Do(av.a, func(data []byte) error {
			t.assert("data == r", bytes.Equal(data, av.value))
			return nil
		}))
	}
	for i, av := range avs {
		t.assert_nil(v.Deref(av.a))
		for _, av := range avs[i+1:] {
			t.assert_nil(v.Do(av.a, func(data []byte) error {
				t.assert("data == r", bytes.Equal(data, av.value))
				return nil
			}))
		}
	}
	t.assert_nil(v.posTree.DoKeys(func(bkey []byte) error {
		key := makeKey(bkey)
		return v.doFree(key, func(free *varFree) error {
			t.Log(bkey, key, free.length, key+uint64(free.length))
			return nil
		})
	}))
	t.assert(fmt.Sprintf("freeLen, %v, <= 5", v.posTree.Size()), v.posTree.Size() <= 5)
	for _, av := range avs {
		a, err := v.Alloc(len(av.value))
		t.assert_nil(err)
		t.assert_nil(v.Do(a, func(data []byte) error {
			copy(data, av.value)
			return nil
		}))
		av.a = a
	}
	for _, av := range avs {
		t.assert_nil(v.Do(av.a, func(data []byte) error {
			t.assert("data == r", bytes.Equal(data, av.value))
			return nil
		}))
	}
}
