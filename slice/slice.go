package slice

import (
	"unsafe"
)

type Slice struct {
	Array unsafe.Pointer
	Len   int
	Cap   int
}

func AsInt16(bytes *[]byte) *int16 {
	s := AsSlice(bytes)
	return (*int16)(s.Array)
}

func AsInt32(bytes *[]byte) *int32 {
	s := AsSlice(bytes)
	return (*int32)(s.Array)
}

func AsInt64(bytes *[]byte) *int64 {
	s := AsSlice(bytes)
	return (*int64)(s.Array)
}

func AsUint16(bytes *[]byte) *uint16 {
	s := AsSlice(bytes)
	return (*uint16)(s.Array)
}

func AsUint32(bytes *[]byte) *uint32 {
	s := AsSlice(bytes)
	return (*uint32)(s.Array)
}

func AsUint64(bytes *[]byte) *uint64 {
	s := AsSlice(bytes)
	return (*uint64)(s.Array)
}

func Uint64AsSlice(i *uint64) []byte {
	s := &Slice{
		Array: unsafe.Pointer(i),
		Len: 8,
		Cap: 8,
	}
	return *s.AsBytes()
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
