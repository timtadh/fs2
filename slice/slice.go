package slice

import (
	"unsafe"
)

type Slice struct {
	Array unsafe.Pointer
	Len   int
	Cap   int
}

func AsSlice(bytes *[]byte) *Slice {
	return (*Slice)(unsafe.Pointer(bytes))
}

func New(sizeInBytes int) *Slice {
	buf := new([]byte)
	*buf = make([]byte, sizeInBytes)
	return AsSlice(buf)
}

func (ss *Slice) AsBytes() *[]byte {
	return (*[]byte)(unsafe.Pointer(ss))
}

func (ss *Slice) AsUint16s() *[]uint16 {
	return (*[]uint16)(unsafe.Pointer(ss))
}

func (ss *Slice) AsUint32s() *[]uint32 {
	return (*[]uint32)(unsafe.Pointer(ss))
}

func (ss *Slice) AsUint64s() *[]uint64 {
	return (*[]uint64)(unsafe.Pointer(ss))
}

func (ss *Slice) AsInt8s() *[]int8 {
	return (*[]int8)(unsafe.Pointer(ss))
}

func (ss *Slice) AsInt16s() *[]int16 {
	return (*[]int16)(unsafe.Pointer(ss))
}

func (ss *Slice) AsInt32s() *[]int32 {
	return (*[]int32)(unsafe.Pointer(ss))
}

func (ss *Slice) AsInt64s() *[]int64 {
	return (*[]int64)(unsafe.Pointer(ss))
}

func (ss *Slice) AsFloat32s() *[]float32 {
	return (*[]float32)(unsafe.Pointer(ss))
}

func (ss *Slice) AsFloat64s() *[]float64 {
	return (*[]float64)(unsafe.Pointer(ss))
}
