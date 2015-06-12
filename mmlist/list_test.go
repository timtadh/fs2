package mmlist

import "testing"

import (
	"os"
	"runtime/debug"
)

import (
	"github.com/timtadh/fs2/fmap"
)

type T testing.T


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


