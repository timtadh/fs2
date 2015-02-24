package bptree

import (
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

type bigValue struct {
	size   uint32
	offset uint64
}

var bvSize uintptr

func init() {
	bvSize = reflect.TypeOf(bigValue{}).Size()
}

func (bv *bigValue) BytesUnsafe() []byte {
	ss := &slice.Slice{
		Array: unsafe.Pointer(bv),
		Len:   int(bvSize),
		Cap:   int(bvSize),
	}
	return *ss.AsBytes()
}

func (bv *bigValue) Bytes() []byte {
	ubytes := bv.BytesUnsafe()
	bytes := make([]byte, len(ubytes))
	copy(bytes, ubytes)
	return bytes
}
