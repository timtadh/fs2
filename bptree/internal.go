package bptree

import (
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

type baseMeta struct {
	flags flag
	keySize uint16
	keyCount uint16
	keyCap uint16
}

type internal struct {
	back []byte
	meta *baseMeta
	keys [][]byte
	ptrs []uint64
}

func loadBaseMeta(backing []byte) *baseMeta {
	back := slice.AsSlice(&backing)
	return (*baseMeta)(back.Array)
}

func (m *baseMeta) Init(flags flag, keySize, keyCap uint16) {
	m.flags = flags
	m.keySize = keySize
	m.keyCount = 0
	m.keyCap = keyCap
}

func (m *baseMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *baseMeta) String() string {
	return fmt.Sprintf(
		"flags: %v, keySize: %v, keyCount: %v, keyCap: %v",
			m.flags, m.keySize, m.keyCount, m.keyCap)
}

func (n *internal) String() string {
	return fmt.Sprintf(
		"meta: <%v>, keys: <%d, %v>, ptrs: <%d, %v>",
			n.meta, len(n.keys), n.keys, len(n.ptrs), n.ptrs)
}

func loadInternal(backing []byte) (*internal, error) {
	meta := loadBaseMeta(backing)
	if meta.flags & INTERNAL == 0 {
		return nil, fmt.Errorf("Was not an internal node")
	}
	return attachInternal(backing, meta)
}

func newInternal(backing []byte, keySize uint16) (*internal, error) {
	meta := loadBaseMeta(backing)

	available := uintptr(len(backing)) - meta.Size()
	ptrSize := uintptr(8)
	kvSize := uintptr(keySize) + ptrSize
	keyCap := uint16(available/kvSize)
	meta.Init(INTERNAL, keySize, keyCap)

	return attachInternal(backing, meta)
}

func attachInternal(backing []byte, meta *baseMeta) (*internal, error) {
	back := slice.AsSlice(&backing)
	base := uintptr(back.Array) + meta.Size()
	keys := make([][]byte, meta.keyCap)
	for i := uintptr(0); i < uintptr(meta.keyCap); i++ {
		s := &slice.Slice{
			Array: unsafe.Pointer(base + i*uintptr(meta.keySize)),
			Len: int(meta.keySize),
			Cap: int(meta.keySize),
		}
		keys[i] = *s.AsBytes()
	}
	ptrs_s := &slice.Slice{
		Array: unsafe.Pointer(base + uintptr(meta.keyCap)*uintptr(meta.keySize)),
		Len: int(meta.keyCap),
		Cap: int(meta.keyCap),
	}
	ptrs := *ptrs_s.AsUint64s()
	return &internal{
		back: backing,
		meta: meta,
		keys: keys,
		ptrs: ptrs,
	}, nil
}

