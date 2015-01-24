package main

import (
	"fmt"
	"unsafe"
	"reflect"
)

const BLOCKSIZE = 127

type slice struct {
	array unsafe.Pointer
	len   int
	cap   int
}

func asSlice(bytes *[]byte) *slice {
	return (*slice)(unsafe.Pointer(bytes))
}

func newSlice() *slice {
	buf := new([]byte)
	*buf = make([]byte, BLOCKSIZE)
	return asSlice(buf)
}

func (ss *slice) asBytes() *[]byte {
	return (*[]byte)(unsafe.Pointer(ss))
}

func (ss *slice) asUint16s() *[]uint16 {
	return (*[]uint16)(unsafe.Pointer(ss))
}

func (ss *slice) asUint64s() *[]uint64 {
	return (*[]uint64)(unsafe.Pointer(ss))
}

type flag uint8

const INTERNAL flag = 0
const (
	LEAF flag = 1 << iota
	BIG_LEAF
	BIG_CHAIN
)

type baseMeta struct {
	flags flag
	keySize uint16
	keyCount uint16
	keyCap uint16
}

type leafMeta struct {
	baseMeta
	next uint64
	prev uint64
}

type bigMeta struct {
	valueSize uint32
	byteCount uint16
	nextPart uint64
}

type bigLeafMeta struct {
	leafMeta
	bigMeta
}

type bigChainMeta struct {
	baseMeta
	bigMeta
}

type internal struct {
	back []byte
	meta *baseMeta
	keys [][]byte
	ptrs []uint64
}

type leaf struct {
	back []byte
	meta *leafMeta
	valueSizes []uint16
	next_kv uintptr
	end uintptr
	keys [][]byte
	vals [][]byte
}

type bigLeaf struct {
	back []byte
	meta *bigLeafMeta
	key []byte
	bytes []byte
}

type bigChain struct {
	back []byte
	meta *bigChainMeta
	bytes []byte
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

func (m *leafMeta) Init(flags flag, keySize, keyCap uint16) {
	bm := &m.baseMeta
	bm.Init(flags, keySize, keyCap)
	m.next = 0
	m.prev = 0
}

func (m *leafMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *leafMeta) String() string {
	return fmt.Sprintf(
		"%v, next: %v, prev: %v",
			&m.baseMeta, m.next, m.prev)
}

func (n *leaf) String() string {
	return fmt.Sprintf(
		"meta: <%v>, valueSizes: <%d, %v>, keys: <%d, %v>, vals: %v",
			n.meta, len(n.valueSizes), n.valueSizes, len(n.keys), n.keys, n.vals)
}

func (m *bigMeta) Init(valueSize uint32, byteCount uint16, nextPart uint64) {
	m.valueSize = valueSize
	m.byteCount = byteCount
	m.nextPart = nextPart
}

func (m *bigMeta) String() string {
	return fmt.Sprintf(
		"valueSize: %v, byteCount: %v, nextPart: %v",
			m.valueSize, m.byteCount, m.nextPart)
}

func (m *bigLeafMeta) Init(flags flag, keySize, keyCap uint16, valueSize uint32, byteCount uint16, nextPart uint64) {
	m.leafMeta.Init(flags, keySize, keyCap)
	m.bigMeta.Init(valueSize, byteCount, nextPart)
}

func (m *bigLeafMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *bigLeafMeta) String() string {
	return fmt.Sprintf(
		"%v, %v",
			&m.leafMeta, &m.bigMeta)
}

func (n *bigLeaf) String() string {
	return fmt.Sprintf(
		"meta: <%v>, key: %v, bytes: %v",
			n.meta, n.key, n.bytes)
}

func (m *bigChainMeta) Init(flags flag, keySize, keyCap uint16, valueSize uint32, byteCount uint16, nextPart uint64) {
	m.baseMeta.Init(flags, keySize, keyCap)
	m.bigMeta.Init(valueSize, byteCount, nextPart)
}

func (m *bigChainMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *bigChainMeta) String() string {
	return fmt.Sprintf(
		"%v, %v",
			&m.baseMeta, &m.bigMeta)
}

func (n *bigChain) String() string {
	return fmt.Sprintf(
		"meta: <%v>, bytes: %v",
			n.meta, n.bytes[:10])
}

func newInternal(keySize uint16) *internal {
	back := newSlice()
	meta := (*baseMeta)(back.array)

	base := uintptr(back.array)
	available := uintptr(back.len) - meta.Size()
	ptrSize := uintptr(8)
	kvSize := uintptr(keySize) + ptrSize
	keyCap := uintptr(available)/kvSize

	meta.Init(INTERNAL, keySize, uint16(keyCap))

	keys := make([][]byte, keyCap)
	for i := uintptr(0); i < keyCap; i++ {
		s := &slice{
			array: unsafe.Pointer(base + meta.Size() + i*uintptr(keySize)),
			len: int(keySize),
			cap: int(keySize),
		}
		keys[i] = *s.asBytes()
	}

	ptrs_s := &slice{
		array: unsafe.Pointer(base + meta.Size() + keyCap*uintptr(keySize)),
		len: int(keyCap),
		cap: int(keyCap),
	}
	ptrs := *ptrs_s.asUint64s()

	return &internal{
		back: *back.asBytes(),
		meta: meta,
		keys: keys,
		ptrs: ptrs,
	}
}

func newLeaf(keySize uint16) *leaf {
	back := newSlice()
	meta := (*leafMeta)(back.array)

	ptr := uintptr(back.array) + meta.Size()
	end := uintptr(back.array) + uintptr(back.cap)
	available := uintptr(back.len) - meta.Size()

	//             size + 1 byte
	valMin := uintptr(2 + 1)
	kvSize := uintptr(keySize) + valMin
	keyCap := uintptr(available)/kvSize

	meta.Init(LEAF, keySize, uint16(keyCap))

	valueSizes_s := &slice{
		array: unsafe.Pointer(ptr),
		len: int(keyCap),
		cap: int(keyCap),
	}
	ptr = ptr + keyCap*2
	valueSizes := *valueSizes_s.asUint16s()
	keys := make([][]byte, keyCap)
	vals := make([][]byte, keyCap)
	return &leaf{
		back: *back.asBytes(),
		meta: meta,
		valueSizes: valueSizes,
		next_kv: ptr,
		end: end,
		keys: keys,
		vals: vals,
	}
}

func newBigLeaf(keySize uint16, valSize uint32) *bigLeaf {
	back := newSlice()
	meta := (*bigLeafMeta)(back.array)

	ptr := uintptr(back.array) + meta.Size()
	end := uintptr(back.array) + uintptr(back.cap)
	byteCount := uint16(end - ptr) - keySize

	meta.Init(BIG_LEAF, keySize, 1, valSize, byteCount, 0)

	key_s := &slice {
		array: unsafe.Pointer(ptr),
		len: int(keySize),
		cap: int(keySize),
	}
	key := *key_s.asBytes()
	ptr = ptr + uintptr(keySize)

	bytes_s := &slice{
		array: unsafe.Pointer(ptr),
		len: int(byteCount),
		cap: int(byteCount),
	}
	bytes := *bytes_s.asBytes()

	return &bigLeaf{
		back: *back.asBytes(),
		meta: meta,
		key: key,
		bytes: bytes,
	}
}

func newBigChain(keySize uint16, valSize uint32) *bigChain {
	back := newSlice()
	meta := (*bigChainMeta)(back.array)

	ptr := uintptr(back.array) + meta.Size()
	end := uintptr(back.array) + uintptr(back.cap)
	byteCount := uint16(end - ptr)

	meta.Init(BIG_LEAF, keySize, 1, valSize, byteCount, 0)

	bytes_s := &slice{
		array: unsafe.Pointer(ptr),
		len: int(byteCount),
		cap: int(byteCount),
	}
	bytes := *bytes_s.asBytes()

	return &bigChain{
		back: *back.asBytes(),
		meta: meta,
		bytes: bytes,
	}
}

func main() {
	fmt.Println("hello")
	n := newInternal(16)
	fmt.Println(n.meta.Size())
	fmt.Println(2*3 + 1)
	fmt.Println(n)
	fmt.Println(n.back[:25])
	n.keys[0][0] = 1
	n.keys[n.meta.keyCap-1][15] = 0xf
	n.ptrs[0] = 1
	n.ptrs[1] = 21
	n.ptrs[2] = 23
	n.ptrs[3] = 125
	n.ptrs[n.meta.keyCap-1] = 0xffffffffffffffff
	fmt.Println(n)
	fmt.Println(n.back[:25])
	startPtrs := int(n.meta.Size())+(int(n.meta.keySize)*int(n.meta.keyCap))
	fmt.Println(n.back[startPtrs-1])
	fmt.Println(n.back[startPtrs:startPtrs+8*4])
	fmt.Println(n.back[len(n.back)-(8*4):])
	end := int(n.meta.Size())+(int(n.meta.keySize)*int(n.meta.keyCap)) + (8*int(n.meta.keyCap))
	fmt.Println(end)
	fmt.Println(len(n.back))
	fmt.Println(len(n.back)-end)
	fmt.Println(n.back)
	fmt.Println(n.back[end-1])

	fmt.Println("\n\n\nleaf times\n\n\n\n")

	l := newLeaf(16)
	for i := range l.valueSizes {
		l.valueSizes[i] = uint16(i)
	}
	fmt.Println(l)
	fmt.Println(l.back)
	fmt.Println(len(l.back))

	fmt.Println("\n\n\nbig leaf times\n\n\n\n")

	bl := newBigLeaf(16, BLOCKSIZE*3 + 47)
	bl.key[0] = 1
	bl.key[15] = 15
	bl.bytes[0] = 1
	bl.bytes[len(bl.bytes)-1] = 15
	fmt.Println(bl)
	fmt.Println(bl.back)
	fmt.Println(len(bl.back))

	fmt.Println("\n\n\nbig chain times\n\n\n\n")

	bc := newBigChain(16, BLOCKSIZE*3 + 47)
	bc.bytes[0] = 1
	bc.bytes[len(bc.bytes)-1] = 15
	fmt.Println(bc)
	fmt.Println(bc.back)
	fmt.Println(len(bc.back))

}
