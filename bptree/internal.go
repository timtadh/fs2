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

var baseMetaSize int

func init() {
	m := &baseMeta{}
	baseMetaSize = int(m.Size())
}

type internal struct {
	back []byte
	meta *baseMeta
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
		"meta: <%v>, ptrs: <%d, %v>",
		n.meta, len(n.ptrs), n.ptrs[:n.meta.keyCount])
}

func (n *internal) Has(key []byte) bool {
	_, has := find(n, key)
	return has
}

func (n *internal) key(i int) []byte {

	keySize := int(n.meta.keySize)
	s := baseMetaSize + i*keySize
	e := s + keySize
	return n.back[s:e]
}

func (n *internal) keyCount() int {
	return int(n.meta.keyCount)
}

func (n *internal) full() bool {
	return n.meta.keyCount+1 >= n.meta.keyCap
}

func (n *internal) ptr(key []byte) (uint64, error) {
	i, has := find(n, key)
	if !has {
		return 0, errors.Errorf("key was not in the internal node")
	}
	return n.ptrs[i], nil
}

func (n *internal) putKP(key []byte, p uint64) error {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.full() {
		return errors.Errorf("block is full")
	}
	err := n.putKey(key, func(i int) error {
		chunk_size := int(n.meta.keyCount) - i
		from := n.ptrs[i : i+chunk_size]
		to := n.ptrs[i+1 : i+chunk_size+1]
		copy(to, from)
		n.ptrs[i] = p
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
	size := int(n.meta.keyCount) - i - 1
	from := n.ptrs[i+1 : i+1+size]
	to := n.ptrs[i : i+size]
	copy(to, from)
	n.ptrs[n.meta.keyCount-1] = 0
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
	meta := loadBaseMeta(backing)
	if meta.flags&iNTERNAL == 0 {
		return nil, errors.Errorf("Was not an internal node")
	}
	return attachInternal(backing, meta)
}

func keysPerInternal(blockSize int, keySize int) int {
	available := blockSize - int((&baseMeta{}).Size())
	ptrSize := 8
	kvSize := keySize + ptrSize
	keyCap := available / kvSize
	return keyCap
}

func newInternal(backing []byte, keySize uint16) (*internal, error) {
	meta := loadBaseMeta(backing)

	available := uintptr(len(backing)) - meta.Size()
	ptrSize := uintptr(8)
	kvSize := uintptr(keySize) + ptrSize
	keyCap := uint16(available / kvSize)
	meta.Init(iNTERNAL, keySize, keyCap)

	return attachInternal(backing, meta)
}

var internSliceBuf chan [][]byte

func init() {
	internSliceBuf = make(chan [][]byte, 100)
}

// note capacity is a *request* there is no guarrantee this function
// will fullfil it. The length will be set to zero
func getInternSliceBytes(capacity int) [][]byte {
	select {
	case s := <-internSliceBuf:
		return s[:0]
	default:
		return make([][]byte, 0, capacity)
	}
}

func relInternSliceBytes(s [][]byte) {
	select {
	case internSliceBuf <- s:
	default:
	}
}

func attachInternal(backing []byte, meta *baseMeta) (*internal, error) {
	back := slice.AsSlice(&backing)
	base := uintptr(back.Array) + meta.Size()
	ptrs_s := &slice.Slice{
		Array: unsafe.Pointer(base + uintptr(meta.keyCap)*uintptr(meta.keySize)),
		Len:   int(meta.keyCap),
		Cap:   int(meta.keyCap),
	}
	ptrs := *ptrs_s.AsUint64s()
	return &internal{
		back: backing,
		meta: meta,
		ptrs: ptrs,
	}, nil
}

func (n *internal) release() {
}
