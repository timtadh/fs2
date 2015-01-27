package bptree

import (
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

type leafMeta struct {
	baseMeta
	next uint64
	prev uint64
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

func loadLeafMeta(backing []byte) *leafMeta {
	back := slice.AsSlice(&backing)
	return (*leafMeta)(back.Array)
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

func (n *leaf) fits(value []byte) bool {
	total_size := int(n.meta.keySize) + len(value)
	return uintptr(total_size) + n.next_kv < end
}

func (n *leaf) putKV(key []byte, value []byte) error {
	if len(key) != int(n.meta.keySize) {
		return fmt.Errorf("key was the wrong size")
	}
	if n.meta.keyCount + 1 >= n.meta.keyCap {
		return fmt.Errorf("block is full")
	}
	if !n.fits(value) {
		return fmt.Errorf("block is full")
	}
	// Tim You Are Here!
	// the situtation.
	// you put the keys and values all mixed up together.
	// the problem is the values are not of equal sizes. this allows you
	// to "pack" these guys in very tightly. However, maintaining sorted
	// order is no longer simple. Need to rethink this a bit.
	err = putKey(int(n.meta.keyCount), n.keys, key, func(i int) error {
		
	})
	if err != nil {
		return err
	}
	n.meta.keyCount += 1
}

func loadLeaf(backing []byte) (*leaf, error) {
	meta := loadLeafMeta(backing)
	if meta.flags & LEAF == 0 {
		return nil, fmt.Errorf("Was not a leaf node")
	}
	return attachLeaf(backing, meta)
}

func newLeaf(backing []byte, keySize uint16) (*leaf, error) {
	meta := loadLeafMeta(backing)

	available := uintptr(len(backing)) - meta.Size()

	// best case: values are all 1 byte
	//             size + 1 byte
	valMin := uintptr(2 + 1)
	kvSize := uintptr(keySize) + valMin
	keyCap := uintptr(available)/kvSize

	meta.Init(LEAF, keySize, uint16(keyCap))
	return attachLeaf(backing, meta)
}

func attachLeaf(backing []byte, meta *leafMeta) (*leaf, error) {
	back := slice.AsSlice(&backing)
	ptr := uintptr(back.Array) + meta.Size()
	end := uintptr(back.Array) + uintptr(back.Cap)
	valueSizes_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: int(meta.keyCap),
		Cap: int(meta.keyCap),
	}
	ptr = ptr + uintptr(meta.keyCap)*2
	valueSizes := *valueSizes_s.AsUint16s()
	keys := make([][]byte, 0, meta.keyCap)
	vals := make([][]byte, 0, meta.keyCap)

	for i := uint16(0); i < meta.keyCount; i++ {
		if ptr >= end {
			break;
		}
		vSize := valueSizes[i]
		key_s := &slice.Slice{
			Array: unsafe.Pointer(ptr),
			Len: int(meta.keySize),
			Cap: int(meta.keySize),
		}
		ptr += uintptr(meta.keySize)
		value_s := &slice.Slice{
			Array: unsafe.Pointer(ptr),
			Len: int(vSize),
			Cap: int(vSize),
		}
		ptr += uintptr(vSize)

		keys = append(keys, *key_s.AsBytes())
		vals = append(vals, *value_s.AsBytes())
	}

	return &leaf{
		back: backing,
		meta: meta,
		valueSizes: valueSizes,
		next_kv: ptr,
		end: end,
		keys: keys,
		vals: vals,
	}, nil
}

