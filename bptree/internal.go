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

func (n *internal) Has(key []byte) bool {
	_, has := find(int(n.meta.keyCount), n.keys, key)
	return has
}

func (n *internal) ptr(key []byte) (uint64, error) {
	i, has := find(int(n.meta.keyCount), n.keys, key)
	if !has {
		return 0, Errorf("key was not in the internal node")
	}
	return n.ptrs[i], nil
}

func (n *internal) putKP(key []byte, p uint64) error {
	if len(key) != int(n.meta.keySize) {
		return Errorf("key was the wrong size")
	}
	if n.meta.keyCount + 1 >= n.meta.keyCap {
		return Errorf("block is full")
	}
	err := putKey(int(n.meta.keyCount), n.keys, key, func(i int) error {
		chunk_size := int(n.meta.keyCount) - i
		from := n.ptrs[i:i+chunk_size]
		to := n.ptrs[i+1:i+chunk_size+1]
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
	i, has := find(int(n.meta.keyCount), n.keys, key)
	if !has {
		return Errorf("key was not in the internal node")
	} else if i < 0 {
		return Errorf("find returned a negative int")
	} else if i >= int(n.meta.keyCount) {
		return Errorf("find returned a int > than len(keys)")
	}
	// remove the key
	err := delItemAt(int(n.meta.keyCount), n.keys, i)
	if err != nil {
		return err
	}
	// remove the ptr
	size := int(n.meta.keyCount) - i - 1
	from := n.ptrs[i+1:i+1+size]
	to := n.ptrs[i:i+size]
	copy(to, from)
	n.ptrs[n.meta.keyCount-1] = 0
	// do the book keeping
	n.meta.keyCount--
	return nil
}

func putKey(keyCount int, keys [][]byte, key []byte, put func(i int) error) error {
	if keyCount + 1 >= len(keys) {
		return Errorf("Block is full.")
	}
	i, has := find(keyCount, keys, key)
	if i < 0 {
		return Errorf("find returned a negative int")
	} else if i >= len(keys) {
		return Errorf("find returned a int > than len(keys)")
	} else if has {
		return Errorf("would have inserted a duplicate key")
	}
	if err := putItemAt(keyCount, keys, key, i); err != nil {
		return err
	}
	return put(i)
}

func putItemAt(itemCount int, items [][]byte, item []byte, i int) error {
	if itemCount + 1 >= len(items) {
		return Errorf("The items slice is full")
	}
	if i < 0 || i > itemCount {
		return Errorf("i was not in range")
	}
	for j := itemCount + 1; j > i; j-- {
		copy(items[j], items[j-1])
	}
	copy(items[i], item)
	return nil
}

func delItemAt(itemCount int, items [][]byte, i int) error {
	if itemCount == 0 {
		return Errorf("The items slice is empty")
	}
	if i < 0 || i >= itemCount {
		return Errorf("i was not in range")
	}
	for j := i; j + 1 < itemCount; j++ {
		copy(items[j], items[j+1])
	}
	// zero the old
	copy(items[itemCount-1], make([]byte, len(items[itemCount-1])))
	return nil
}

func loadInternal(backing []byte) (*internal, error) {
	meta := loadBaseMeta(backing)
	if meta.flags & INTERNAL == 0 {
		return nil, Errorf("Was not an internal node")
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

