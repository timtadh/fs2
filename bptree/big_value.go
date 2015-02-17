package bptree

import (
	"unsafe"
	"reflect"
)

import (
	"github.com/timtadh/fs2/slice"
)

type bigValue struct {
	size uint32
	offset uint64
}

var bvSize uintptr

func init() {
	bvSize = reflect.TypeOf(bigValue{}).Size()
}

func (self *BpTree) doValue(a uint64, idx int, do func([]byte) error) error {
	return Errorf("Not yet implemented")
}

func (bv *bigValue) BytesUnsafe() []byte {
	ss := &slice.Slice{
		Array: unsafe.Pointer(bv),
		Len: int(bvSize),
		Cap: int(bvSize),
	}
	return *ss.AsBytes()
}

func (bv *bigValue) Bytes() []byte {
	ubytes := bv.BytesUnsafe()
	bytes := make([]byte, len(ubytes))
	copy(bytes, ubytes)
	return bytes
}

