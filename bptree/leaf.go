package bptree

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

type WhereFunc func(value []byte) bool

type leafMeta struct {
	baseMeta
	next uint64
	prev uint64
}

type leaf struct {
	back []byte
	meta *leafMeta
	valueSizes []uint16
	end uintptr
	kvs []byte
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

func (n *leaf) Has(key []byte) bool {
	_, has := find(int(n.meta.keyCount), n.keys, key)
	return has
}

func (n *leaf) next_kv_in_kvs() int {
	return n.keyOffset(int(n.meta.keyCount))
}

func (n *leaf) size(value []byte) int {
	return int(n.meta.keySize) + len(value)
}

func (n *leaf) fits(value []byte) bool {
	size := n.size(value)
	end := n.next_kv_in_kvs()
	return end + size < len(n.kvs)
}

func (n *leaf) keyOffset(idx int) int {
	offset := 0
	for i := 0; i < idx; i++ {
		offset += int(n.meta.keySize)
		offset += int(n.valueSizes[i])
	}
	return offset
}

func (n *leaf) putKV(key []byte, value []byte) error {
	if len(key) != int(n.meta.keySize) {
		return fmt.Errorf("key was the wrong size")
	}
	if n.meta.keyCount + 1 >= n.meta.keyCap {
		return fmt.Errorf("block is full")
	}
	if !n.fits(value) {
		return fmt.Errorf("block is full (value doesn't fit)")
	}
	key_idx, _ := find(int(n.meta.keyCount), n.keys, key)
	key_offset := n.keyOffset(key_idx)
	kv_size := n.size(value)
	length := n.next_kv_in_kvs()
	if key_idx == int(n.meta.keyCount) {
		// fantastic we don't nee to move any thing.
		// we can just append
	} else {
		// we move the valueSizes around to expand
		chunk_size := int(n.meta.keyCount) - key_idx
		from := n.valueSizes[key_idx:key_idx+chunk_size]
		to := n.valueSizes[key_idx+1:key_idx+chunk_size+1]
		copy(to, from)
		// then we make room for the kv
		to_shift := length - key_offset
		shift(n.kvs, key_offset, to_shift, kv_size, true)
	}
	// do the book keeping
	n.valueSizes[key_idx] = uint16(len(value))
	n.meta.keyCount += 1
	// copy in the new kv
	key_end := key_offset+int(n.meta.keySize)
	key_slice := n.kvs[key_offset:key_end]
	val_slice := n.kvs[key_end:key_end+len(value)]
	copy(key_slice, key)
	copy(val_slice, value)
	// reattach our byte slices
	return n.reattachLeaf()
}

func (n *leaf) delKV(key []byte, where WhereFunc) error {
	if len(key) != int(n.meta.keySize) {
		return fmt.Errorf("key was the wrong size")
	}
	if n.meta.keyCount <= 0 {
		return fmt.Errorf("block is empty")
	}
	key_idx, has := find(int(n.meta.keyCount), n.keys, key)
	if !has {
		return fmt.Errorf("that key was not in the block")
	}
	for ; key_idx < int(n.meta.keyCount); key_idx++ {
		if !bytes.Equal(key, n.keys[key_idx]) {
			return nil
		}
		if where(n.vals[key_idx]) {
			break
		}
	}
	// ok we have our key_idx
	length := n.next_kv_in_kvs()
	if key_idx + 1 == int(n.meta.keyCount) {
		// sweet we can just drop the last
		// key value
		n.valueSizes[key_idx] = 0
		n.meta.keyCount--
		return n.reattachLeaf()
	}
	// drop the k4
	{
		key_offset := n.keyOffset(key_idx)
		i := n.keyOffset(key_idx+1)
		size := length - i
		shift(n.kvs, i, size, i - key_offset, false)
	}
	// drop the valueSize
	{
		size := int(n.meta.keyCount) - key_idx - 1
		from := n.valueSizes[key_idx+1:key_idx+1+size]
		to := n.valueSizes[key_idx:key_idx+size]
		copy(to, from)
		n.valueSizes[n.meta.keyCount-1] = 0
	}
	// do the book keeping
	n.meta.keyCount--
	// retattach the leaf
	return n.reattachLeaf()
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
	// the size value is the size of the valueLength
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

	kvs_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: int(end - ptr),
		Cap: int(end - ptr),
	}
	kvs := *kvs_s.AsBytes()

	node := &leaf{
		back: backing,
		meta: meta,
		valueSizes: valueSizes,
		end: end,
		kvs: kvs,
	}

	err := node.reattachLeaf()
	if err != nil {
		return nil, err
	}
	return node, nil
}


func (n *leaf) reattachLeaf() error {
	kvs_s := slice.AsSlice(&n.kvs)
	ptr := uintptr(kvs_s.Array)
	end := ptr + uintptr(kvs_s.Len)

	keys := make([][]byte, 0, n.meta.keyCap)
	vals := make([][]byte, 0, n.meta.keyCap)

	for i := uint16(0); i < n.meta.keyCount; i++ {
		if ptr >= end {
			return fmt.Errorf("overran backing array on reattachLeaf()")
		}
		vSize := n.valueSizes[i]
		key_s := &slice.Slice{
			Array: unsafe.Pointer(ptr),
			Len: int(n.meta.keySize),
			Cap: int(n.meta.keySize),
		}
		ptr += uintptr(n.meta.keySize)
		value_s := &slice.Slice{
			Array: unsafe.Pointer(ptr),
			Len: int(vSize),
			Cap: int(vSize),
		}
		ptr += uintptr(vSize)

		keys = append(keys, *key_s.AsBytes())
		vals = append(vals, *value_s.AsBytes())
	}

	n.keys = keys
	n.vals = vals

	return nil
}


