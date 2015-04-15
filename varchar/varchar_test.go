package varchar

import "testing"

import (
	"bytes"
	"os"
	"runtime/debug"
	"fmt"
)

import (
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

const TESTS = 500

type T testing.T

type AV struct {
	a uint64
	value []byte
}

func (t *T) blkfile() (*fmap.BlockFile, func()) {
	bf, err := fmap.Anonymous(fmap.BLOCKSIZE)
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

func (t *T) varchar() (*Varchar, func()) {
	bf, clean := t.blkfile()
	a, err := bf.Allocate()
	t.assert_nil(err)
	v, err := New(bf, a)
	t.assert_nil(err)
	return v, clean
}

func (t *T) assert(msg string, oks ...bool) {
	for _, ok := range oks {
		if !ok {
			t.Log("\n" + string(debug.Stack()))
			t.Error(msg)
			t.Fatal("assert failed")
		}
	}
}

func (t *T) assert_nil(errors ...error) {
	for _, err := range errors {
		if err != nil {
			t.Log("\n" + string(debug.Stack()))
			t.Fatal(err)
		}
	}
}

func (t *T) rand_bytes(length int) []byte {
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

func (t *T) rand_varchar(min, max int) []byte {
	bytes := t.rand_bytes(4)
	s := slice.AsSlice(&bytes)
	length := int(*(*uint32)(s.Array))
	length = (length % (max)) + min
	return t.rand_bytes(length)
}

func TestNew(x *testing.T) {
	t := (*T)(x)
	v, clean := t.varchar()
	defer clean()
	_, err := v.Alloc(128)
	t.assert_nil(err)
}

func TestOpen(x *testing.T) {
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
	v2, err := Open(v.bf, v.a)
	t.assert_nil(err)
	t.assert_nil(v2.Do(a, func(data []byte) error {
		t.assert("data == r", bytes.Equal(data, r))
		return nil
	}))
}

func TestAlloc(x *testing.T) {
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

func TestFree(x *testing.T) {
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
	var freeLen int
	t.assert_nil(v.doCtrl(func(ctrl *varCtrl) error {
		freeLen = int(ctrl.posList.len)
		return nil
	}))
	for i, av := range avs {
		t.assert_nil(v.Deref(av.a))
		for _, av := range avs[i+1:] {
			t.assert_nil(v.Do(av.a, func(data []byte) error {
				t.assert("data == r", bytes.Equal(data, av.value))
				return nil
			}))
		}
	}
	t.assert_nil(v.doCtrl(func(ctrl *varCtrl) error {
		t.assert(fmt.Sprintf("freeLen, %v, <= 1", ctrl.posList.len), ctrl.posList.len <= 1)
		return nil
		/*
		return v.doFree(ctrl.freeHead, func(m *varFree) error {
			t.assert("head.Len >= sum", m.length >= uint32(sum))
			return nil
		})*/
	}))
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

