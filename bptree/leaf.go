package bptree

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/fmap"
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
	valueFlags []uint8
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

func (n *leaf) doValue(bf *fmap.BlockFile, key []byte, do func([]byte) error) error {
	i, has := find(int(n.meta.keyCount), n.keys, key)
	if !has {
		return Errorf("key was not in the leaf node")
	}
	switch flag(n.valueFlags[i]) {
	case 0: return Errorf("Unset value flag")
	case SMALL_VALUE: return do(n.vals[i])
	case BIG_VALUE: return n.doBigValue(bf, i, do)
	default: return Errorf("Unexpected value type")
	}
}

func (n *leaf) doBigValue(bf *fmap.BlockFile, i int, do func([]byte) error) error {
	bv_bytes := make([]byte, bvSize)
	copy(bv_bytes, n.vals[i])
	bv := (*bigValue)(slice.AsSlice(&bv_bytes).Array)
	blks := n.blksNeeded(bf, int(bv.size))
	if bv.offset == 0 {
		return Errorf("the bv.offset, %d, was 0.", bv.offset)
	} else if bv.offset % 4096 != 0 {
		return Errorf("the bv.offset, %d, was not block aligned", bv.offset)
	}
	return bf.Do(bv.offset, uint64(blks), func(bytes []byte) error {
		return do(bytes[:bv.size])
	})
}

func (n *leaf) first_value(bf *fmap.BlockFile, key []byte) (value []byte, err error) {
	err = n.doValue(bf, key, func(bytes []byte) error {
		value = make([]byte, len(bytes))
		copy(value, bytes)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (n *leaf) next_kv_in_kvs() int {
	return n.keyOffset(int(n.meta.keyCount))
}

func (n *leaf) size(bf *fmap.BlockFile, value []byte) int {
	if len(value) > int(bf.BlockSize()) / 4 {
		return int(n.meta.keySize) + int(bvSize)
	}
	return int(n.meta.keySize) + len(value)
}

func (n *leaf) fits(bf *fmap.BlockFile, value []byte) bool {
	size := n.size(bf, value)
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

func (n *leaf) pure() bool {
	if n.meta.keyCount == 0 {
		return true
	}
	key := n.keys[0]
	for i := 1; i < int(n.meta.keyCount); i++ {
		if !bytes.Equal(key, n.keys[i]) {
			return false
		}
	}
	return true
}

func (n *leaf) makeBigValue(bf *fmap.BlockFile, value []byte) (bigVal []byte, err error) {
	N := n.blksNeeded(bf, len(value))
	a, err := bf.AllocateBlocks(N)
	if err != nil {
		return nil, err
	}
	err = bf.Do(a, uint64(N), func(bytes []byte) error {
		if len(bytes) < len(value) {
			return Errorf("Did not have enough bytes")
		}
		copy(bytes, value)
		return nil
	})
	if err != nil {
		return nil, err
	}
	var bv *bigValue = &bigValue{
		size: uint32(len(value)),
		offset: a,
	}
	bv_bytes := bv.Bytes()
	nbv := (*bigValue)(slice.AsSlice(&bv_bytes).Array)
	if nbv.offset != bv.offset {
		return nil, Errorf("nvb was wrong on offset")
	}
	if nbv.size != bv.size {
		return nil, Errorf("nvb was wrong on size")
	}
	return bv_bytes, nil
}

func (n *leaf) blksNeeded(bf *fmap.BlockFile, size int) int {
	blk := int(bf.BlockSize())
	m := size % blk
	if m == 0 {
		return size / blk
	}
	return (size + (blk - m))/blk
}

func (n *leaf) putKV(bf *fmap.BlockFile, key []byte, value []byte) (err error) {
	var bigValue bool = false
	if len(key) != int(n.meta.keySize) {
		return Errorf("key was the wrong size")
	}
	if n.meta.keyCount + 1 >= n.meta.keyCap {
		return Errorf("block is full")
	}
	if len(value) > int(bf.BlockSize()) / 4 {
		value, err = n.makeBigValue(bf, value)
		if err != nil {
			return err
		}
		bigValue = true
	}
	if !n.fits(bf, value) {
		return Errorf("block is full (value doesn't fit)")
	}
	key_idx, _ := find(int(n.meta.keyCount), n.keys, key)
	key_offset := n.keyOffset(key_idx)
	kv_size := n.size(bf, value)
	length := n.next_kv_in_kvs()
	if key_idx == int(n.meta.keyCount) {
		// fantastic we don't nee to move any thing.
		// we can just append
	} else {
		chunk_size := int(n.meta.keyCount) - key_idx
		// we move the valueSizes around to expand
		{
			from := n.valueSizes[key_idx:key_idx+chunk_size]
			to := n.valueSizes[key_idx+1:key_idx+chunk_size+1]
			copy(to, from)
		}
		// we move the valueFlags around to expand
		{
			from := n.valueFlags[key_idx:key_idx+chunk_size]
			to := n.valueFlags[key_idx+1:key_idx+chunk_size+1]
			copy(to, from)
		}
		// then we make room for the kv
		to_shift := length - key_offset
		shift(n.kvs, key_offset, to_shift, kv_size, true)
	}
	// do the book keeping
	n.valueSizes[key_idx] = uint16(len(value))
	n.meta.keyCount += 1
	if bigValue {
		n.valueFlags[key_idx] = uint8(BIG_VALUE)
	} else {
		n.valueFlags[key_idx] = uint8(SMALL_VALUE)
	}
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
		return Errorf("key was the wrong size")
	}
	if n.meta.keyCount <= 0 {
		return Errorf("block is empty")
	}
	key_idx, has := find(int(n.meta.keyCount), n.keys, key)
	if !has {
		return Errorf("that key was not in the block")
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
	// drop the valueFlag
	{
		size := int(n.meta.keyCount) - key_idx - 1
		from := n.valueFlags[key_idx+1:key_idx+1+size]
		to := n.valueFlags[key_idx:key_idx+size]
		copy(to, from)
		n.valueFlags[n.meta.keyCount-1] = 0
	}
	// do the book keeping
	n.meta.keyCount--
	// retattach the leaf
	return n.reattachLeaf()
}

func loadLeaf(backing []byte) (*leaf, error) {
	meta := loadLeafMeta(backing)
	if meta.flags & LEAF == 0 {
		return nil, Errorf("Was not a leaf node")
	}
	return attachLeaf(backing, meta)
}

func newLeaf(backing []byte, keySize uint16) (*leaf, error) {
	meta := loadLeafMeta(backing)

	available := uintptr(len(backing)) - meta.Size()

	// best case: values are all 1 byte
	// the size value is the size of the valueLength
	//             size + 1 byte
	valMin := uintptr(2 + 1 + 1)
	kvSize := uintptr(keySize) + valMin
	keyCap := available/kvSize

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

	valueFlags_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: int(meta.keyCap),
		Cap: int(meta.keyCap),
	}
	ptr = ptr + uintptr(meta.keyCap)
	valueFlags := *valueFlags_s.AsBytes()

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
		valueFlags: valueFlags,
		end: end,
		kvs: kvs,
	}
	node.keys = make([][]byte, 0, node.meta.keyCap)
	node.vals = make([][]byte, 0, node.meta.keyCap)

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

	keys := n.keys[:0]
	vals := n.vals[:0]

	for i := uint16(0); i < n.meta.keyCount; i++ {
		if ptr >= end {
			return Errorf("overran backing array on reattachLeaf()")
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


