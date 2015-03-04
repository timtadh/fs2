package bptree

import (
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type baseMeta struct {
	flags    flag
	keySize  uint16
	keyCount uint16
	keyCap   uint16
}

const ptrSize = 8

const baseMetaSize = 8
var baseMetaSizeActual int

func init() {
	m := &baseMeta{}
	baseMetaSizeActual = int(m.Size())
	if baseMetaSizeActual != baseMetaSize {
		panic("the baseMeta was an unexpected size")
	}
}

type internal struct {
	meta baseMeta
	bytes [BLOCKSIZE-baseMetaSize]byte
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
		"meta: <%v>",
		n.meta)
}

func (n *internal) Has(key []byte) bool {
	_, has := find(n, key)
	return has
}

func (n *internal) key(i int) []byte {
	keySize := int(n.meta.keySize)
	s := i*keySize
	e := s + keySize
	return n.bytes[s:e]
}

func (n *internal) ptr(i int) *uint64 {
	ptr := uintptr(unsafe.Pointer(n))
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := baseMetaSize + keyCap*keySize + i*ptrSize
	p := ptr + uintptr(s)
	return (*uint64)(unsafe.Pointer(p))
}

func (n *internal) ptrs() []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := keyCap*keySize
	e := s + keyCap*ptrSize
	return n.bytes[s:e]
}

func (n *internal) keyCount() int {
	return int(n.meta.keyCount)
}

func (n *internal) full() bool {
	return n.meta.keyCount+1 >= n.meta.keyCap
}

func (n *internal) findPtr(key []byte) (uint64, error) {
	i, has := find(n, key)
	if !has {
		return 0, errors.Errorf("key was not in the internal node")
	}
	return *n.ptr(i), nil
}

func (n *internal) putKP(key []byte, p uint64) error {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.full() {
		return errors.Errorf("block is full")
	}
	err := n.putKey(key, func(i int) error {
		ptrs := n.ptrs()
		chunkSize := (int(n.meta.keyCount) - i)*ptrSize
		s := i * ptrSize
		from := ptrs[s : s+chunkSize]
		to := ptrs[s+ptrSize : s+chunkSize+ptrSize]
		copy(to, from)
		*n.ptr(i) = p
		return nil
	})
	if err != nil {
		return err
	}
	n.meta.keyCount++
	return nil
}

func (n *internal) delKP(key []byte) error {
	i, has := find(n, key)
	if !has {
		return errors.Errorf("key was not in the internal node")
	} else if i < 0 {
		return errors.Errorf("find returned a negative int")
	} else if i >= int(n.meta.keyCount) {
		return errors.Errorf("find returned a int > than len(keys)")
	}
	return n.delItemAt(i)
}

func (n *internal) delItemAt(i int) error {
	// remove the key
	err := n.delKeyAt(i)
	if err != nil {
		return err
	}
	// remove the ptr
	ptrs := n.ptrs()
	chunkSize := (int(n.meta.keyCount) - i - 1)*ptrSize
	s := i * ptrSize
	from := ptrs[s+ptrSize : s+ptrSize+chunkSize]
	to := ptrs[s : s+chunkSize]
	copy(to, from)
	*n.ptr(int(n.meta.keyCount-1)) = 0
	// do the book keeping
	n.meta.keyCount--
	return nil
}

func (n *internal) putKey(key []byte, put func(i int) error) error {
	if n.keyCount()+1 >= int(n.meta.keyCap) {
		return errors.Errorf("Block is full.")
	}
	i, has := find(n, key)
	if i < 0 {
		return errors.Errorf("find returned a negative int")
	} else if i >= int(n.meta.keyCap) {
		return errors.Errorf("find returned a int > than len(keys)")
	} else if has {
		return errors.Errorf(fmt.Sprintf("would have inserted a duplicate key, %v", key))
	}
	if err := n.putKeyAt(key, i); err != nil {
		return err
	}
	return put(i)
}

func (n *internal) putKeyAt(key []byte, i int) error {
	if i < 0 || i > int(n.meta.keyCount) {
		return errors.Errorf("i was not in range")
	}
	for j := int(n.meta.keyCount) + 1; j > i; j-- {
		copy(n.key(j), n.key(j-1))
	}
	copy(n.key(i), key)
	return nil
}

func (n *internal) delKeyAt(i int) error {
	if n.meta.keyCount == 0 {
		return errors.Errorf("The items slice is empty")
	}
	if i < 0 || i >= int(n.meta.keyCount) {
		return errors.Errorf("i was not in range")
	}
	for j := i; j+1 < int(n.meta.keyCount); j++ {
		copy(n.key(j), n.key(j+1))
	}
	// zero the old
	fmap.MemClr(n.key(int(n.meta.keyCount-1)))
	return nil
}

func loadInternal(backing []byte) (*internal, error) {
	n := asInternal(backing)
	if n.meta.flags&iNTERNAL == 0 {
		return nil, errors.Errorf("Was not an internal node")
	}
	return n, nil
}

func keysPerInternal(blockSize int, keySize int) int {
	available := blockSize - int((&baseMeta{}).Size())
	ptrSize := 8
	kvSize := keySize + ptrSize
	keyCap := available / kvSize
	return keyCap
}

func asInternal(backing []byte) *internal {
	back := slice.AsSlice(&backing)
	return (*internal)(back.Array)
}

func newInternal(backing []byte, keySize uint16) (*internal, error) {
	n := asInternal(backing)

	keyCap := uint16(keysPerInternal(len(backing), int(keySize)))
	n.meta.Init(iNTERNAL, keySize, keyCap)

	return n, nil
}

func (n *internal) release() {
}
