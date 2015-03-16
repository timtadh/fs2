package bptree

import (
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

type bigValue struct {
	offset uint64
	size   uint32
	refs   uint16
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

func (bv *bigValue) Ref() {
	bv.refs += 1
}

func (bv *bigValue) Deref() {
	bv.refs -= 1
}
