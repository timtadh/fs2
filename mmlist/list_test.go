package mmlist

import "testing"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
)

import (
	"github.com/timtadh/fs2/fmap"
)

type T testing.T

func init() {
	if urandom, err := os.Open("/dev/urandom"); err != nil {
		panic(err)
	} else {
		seed := make([]byte, 8)
		if _, err := urandom.Read(seed); err == nil {
			rand.Seed(int64(binary.BigEndian.Uint64(seed)))
		}
		urandom.Close()
	}
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

func (t *T) mmlist() (*List, func()) {
	bf, clean := t.blkfile()
	l, err := New(bf)
	t.assert_nil(err)
	return l, clean
}

func (t *T) Log(msgs ...interface{}) {
	x := (*testing.T)(t)
	x.Log(msgs...)
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

func TestNew(x *testing.T) {
	t := (*T)(x)
	bf, clean := t.blkfile()
	_, err := New(bf)
	t.assert_nil(err)
	clean()
}

func TestAppend(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	d, err := l.Get(i)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
}

func TestSet(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	d, err := l.Get(i)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	t.assert_nil(l.Set(i, []byte("goodbye")))
	d, err = l.Get(i)
	t.assert_nil(err)
	t.assert("d == goodbye", bytes.Equal(d, []byte("goodbye")))
}

func TestOpen(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	d, err := l.Get(i)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	t.assert_nil(l.Set(i, []byte("goodbye")))
	l, err = Open(l.bf)
	t.assert_nil(err)
	d, err = l.Get(i)
	t.assert_nil(err)
	t.assert("d == goodbye", bytes.Equal(d, []byte("goodbye")))
}

func TestOpenFail(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	l, err = OpenAt(l.bf, 4096*2)
	if err == nil {
		t.Error("should not have been able to Open", l)
	}
}

func TestPop(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	d, err := l.Get(i)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	d, err = l.Pop()
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	t.assert("size == 0", l.Size() == 0)
	_, err = l.Get(i)
	if err == nil {
		t.Fatal("should have not been able to get a popped item")
	}
}

func TestSwap(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	i, err := l.Append([]byte("hello"))
	t.assert_nil(err)
	t.assert("i == 0", i == 0)
	d, err := l.Get(i)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	j, err := l.Append([]byte("goodbye"))
	t.assert_nil(err)
	t.assert("j == 1", j == 1)
	d, err = l.Get(j)
	t.assert_nil(err)
	t.assert("d == goodbye", bytes.Equal(d, []byte("goodbye")))
	t.assert_nil(l.Swap(i, j))
	d, err = l.Get(i)
	t.assert_nil(err)
	t.assert("d == goodbye", bytes.Equal(d, []byte("goodbye")))
	d, err = l.Get(j)
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	d, err = l.SwapDelete(i)
	t.assert_nil(err)
	t.assert("d == goodbye", bytes.Equal(d, []byte("goodbye")))
	d, err = l.Pop()
	t.assert_nil(err)
	t.assert("d == hello", bytes.Equal(d, []byte("hello")))
	t.assert("size == 0", l.Size() == 0)
}

func TestAppendPopCycle(x *testing.T) {
	t := (*T)(x)
	l, clean := t.mmlist()
	defer clean()
	items := make([][]byte, 0, itemsPerIdx*25)
	for i := 0; i < cap(items); i++ {
		items = append(items, t.rand_bytes(rand.Intn(8192)))
	}
	for i, item := range items {
		j, err := l.Append(item)
		t.assert_nil(err)
		t.assert("i == j", uint64(i) == j)
		j, err = l.Append(item)
		t.assert_nil(err)
		j, err = l.Append(item)
		t.assert_nil(err)
		d, err := l.Pop()
		t.assert_nil(err)
		t.assert(fmt.Sprintf("items[i] == d, %d, %d", len(items[i]), len(d)), bytes.Equal(items[i], d))
		d, err = l.Pop()
		t.assert_nil(err)
		t.assert(fmt.Sprintf("items[i] == d, %d, %d", len(items[i]), len(d)), bytes.Equal(items[i], d))
	}
	t.assert("len(items) == l.Size()", len(items) == int(l.Size()))
	for i := uint64(0); i < l.Size(); i++ {
		d, err := l.Get(i)
		t.assert_nil(err)
		t.assert("items[i] == d", bytes.Equal(items[i], d))
	}
	for i := uint64(0); i < l.Size(); i++ {
		d, err := l.Get(i)
		t.assert_nil(err)
		t.assert("items[i] == d", bytes.Equal(items[i], d))
	}
	t.Log("len(items) == l.Size()", len(items), l.Size(), len(items) == int(l.Size()))
	t.Log(l.Size())
	for i := int(l.Size()) - 1; i >= 0; i-- {
		d, err := l.Pop()
		t.assert_nil(err)
		t.assert(fmt.Sprintf("items[i] == d, %d, %d", len(items[i]), len(d)), bytes.Equal(items[i], d))
		t.assert("size == i", int(l.Size()) == i)
	}
	t.assert("size == 0", l.Size() == 0)
}
