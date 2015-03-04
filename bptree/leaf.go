package bptree

import (
	"bytes"
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type leafMeta struct {
	baseMeta
	next uint64
	prev uint64
}

type leaf struct {
	meta       leafMeta
	bytes      [BLOCKSIZE-leafMetaSize]byte
}


const minValSize = 1
const leafMetaSize = 24
var leafMetaSizeActual int

func init() {
	m := &leafMeta{}
	leafMetaSizeActual = int(m.Size())
	if leafMetaSizeActual != leafMetaSize {
		panic("the leafMeta was an unexpected size")
	}
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
		"meta: <%v>",
		n.meta)
}

func (n *leaf) Has(key []byte) bool {
	_, has := find(n, key)
	return has
}

func (n *leaf) key(i int) []byte {
	s := n.keyOffset(i)
	e := s + int(n.meta.keySize)
	kvs := n.kvs()
	return kvs[s:e]
}

func (n *leaf) keyCount() int {
	return int(n.meta.keyCount)
}

func (n *leaf) val(i int) []byte {
	size := int(*n.valueSize(i))
	s := n.keyOffset(i) + int(n.meta.keySize)
	e := s + size
	return n.kvs()[s:e]
}

func (n *leaf) valueSize(i int) *uint16 {
	ptr := uintptr(unsafe.Pointer(n))
	s := leafMetaSize + i*2
	p := ptr + uintptr(s)
	return (*uint16)(unsafe.Pointer(p))
}

func (n *leaf) valueFlag(i int) *flag {
	keyCap := int(n.meta.keyCap)
	s := keyCap*2 + i
	vf := &n.bytes[s]
	return (*flag)(vf)
}

func (n *leaf) valueSizes() []byte {
	keyCap := int(n.meta.keyCap)
	s := 0
	e := s + keyCap*2
	return n.bytes[s:e]
}

func (n *leaf) valueFlags() []byte {
	keyCap := int(n.meta.keyCap)
	s := keyCap*2
	e := s + keyCap*1
	return n.bytes[s:e]
}

func (n *leaf) kvs() []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := keyCap*2 + keyCap*1
	e := s + keyCap*keySize + keyCap*minValSize
	return n.bytes[s:e]
}

func (n *leaf) doValue(bf *fmap.BlockFile, key []byte, do func([]byte) error) error {
	i, has := find(n, key)
	if !has {
		return errors.Errorf("key was not in the leaf node")
	}
	return n.doValueAt(bf, i, do)
}

func (n *leaf) doValueAt(bf *fmap.BlockFile, i int, do func([]byte) error) error {
	switch *n.valueFlag(i) {
	case 0:
		return errors.Errorf("Unset value flag")
	case sMALL_VALUE:
		return do(n.val(i))
	case bIG_VALUE:
		return n.doBigValue(bf, i, do)
	default:
		return errors.Errorf("Unexpected value type")
	}
}

func (n *leaf) doBigValue(bf *fmap.BlockFile, i int, do func([]byte) error) error {
	bv_bytes := make([]byte, bvSize)
	copy(bv_bytes, n.val(i))
	bv := (*bigValue)(slice.AsSlice(&bv_bytes).Array)
	blks := blksNeeded(bf, int(bv.size))
	if bv.offset == 0 {
		return errors.Errorf("the bv.offset, %d, was 0.", bv.offset)
	} else if bv.offset%4096 != 0 {
		return errors.Errorf("the bv.offset, %d, was not block aligned", bv.offset)
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

func (n *leaf) size(value []byte) int {
	return int(n.meta.keySize) + len(value)
}

func (n *leaf) fits(value []byte) bool {
	size := n.size(value)
	end := n.next_kv_in_kvs()
	return (end+size < len(n.kvs())) && n.meta.keyCount+1 < n.meta.keyCap
}

func (n *leaf) keyOffset(idx int) int {
	keyCap := int(n.meta.keyCap)
	e := keyCap*2
	b := n.bytes[:e]
	v := slice.AsSlice(&b)
	v.Cap = keyCap
	v.Len = keyCap
	valueSizes := *v.AsUint16s()
	offset := 0
	for i := 0; i < idx; i++ {
		offset += int(n.meta.keySize)
		offset += int(valueSizes[i])
	}
	return offset
}

func (n *leaf) pure() bool {
	if n.meta.keyCount == 0 {
		return true
	}
	key := n.key(0)
	for i := 1; i < int(n.meta.keyCount); i++ {
		if !bytes.Equal(key, n.key(i)) {
			return false
		}
	}
	return true
}

func (n *leaf) putKV(valFlags flag, key []byte, value []byte) (err error) {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.meta.keyCount+1 >= n.meta.keyCap {
		return errors.Errorf("block is full")
	}
	if !n.fits(value) {
		return errors.Errorf("block is full (value doesn't fit)")
	}
	key_idx, _ := find(n, key)
	key_offset := n.keyOffset(key_idx)
	kv_size := n.size(value)
	length := n.next_kv_in_kvs()
	if key_idx == int(n.meta.keyCount) {
		// fantastic we don't nee to move any thing.
		// we can just append
	} else {
		chunk_size := int(n.meta.keyCount) - key_idx
		// we move the valueSizes around to expand
		{
			valueSizes := n.valueSizes()
			s := key_idx*2
			size := chunk_size*2
			from := valueSizes[s : s+size]
			to := valueSizes[s+2 : s+size+2]
			copy(to, from)
		}
		// we move the valueFlags around to expand
		{
			valueFlags := n.valueFlags()
			from := valueFlags[key_idx : key_idx+chunk_size]
			to := valueFlags[key_idx+1 : key_idx+chunk_size+1]
			copy(to, from)
		}
		// then we make room for the kv
		to_shift := length - key_offset
		shift(n.kvs(), key_offset, to_shift, kv_size, true)
	}
	// do the book keeping
	*n.valueSize(key_idx) = uint16(len(value))
	n.meta.keyCount += 1
	*n.valueFlag(key_idx) = valFlags
	// copy in the new kv
	kvs := n.kvs()
	key_end := key_offset + int(n.meta.keySize)
	key_slice := kvs[key_offset:key_end]
	val_slice := kvs[key_end : key_end+len(value)]
	copy(key_slice, key)
	copy(val_slice, value)
	// reattach our byte slices
	return nil
}

func (n *leaf) delKV(key []byte, which func([]byte) bool) error {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.meta.keyCount <= 0 {
		return errors.Errorf("block is empty")
	}
	key_idx, has := find(n, key)
	if !has {
		return errors.Errorf("that key was not in the block")
	}
	for ; key_idx < int(n.meta.keyCount); key_idx++ {
		if !bytes.Equal(key, n.key(key_idx)) {
			return nil
		}
		if which(n.val(key_idx)) {
			break
		}
	}
	return n.delItemAt(key_idx)
}

func (n *leaf) delItemAt(key_idx int) error {
	// ok we have our key_idx
	length := n.next_kv_in_kvs()
	if key_idx+1 == int(n.meta.keyCount) {
		// sweet we can just drop the last
		// key value
		*n.valueSize(key_idx) = 0
		n.meta.keyCount--
		return nil
	}
	// drop the k4
	{
		key_offset := n.keyOffset(key_idx)
		i := n.keyOffset(key_idx + 1)
		size := length - i
		shift(n.kvs(), i, size, i-key_offset, false)
	}
	// drop the valueSize
	{
		valueSizes := n.valueSizes()
		size := (int(n.meta.keyCount) - key_idx - 1)*2
		s := key_idx*2
		from := valueSizes[s+2 : s+2+size]
		to := valueSizes[s : s+size]
		copy(to, from)
		*n.valueSize(int(n.meta.keyCount-1)) = 0
	}
	// drop the valueFlag
	{
		valueFlags := n.valueFlags()
		size := int(n.meta.keyCount) - key_idx - 1
		from := valueFlags[key_idx+1 : key_idx+1+size]
		to := valueFlags[key_idx : key_idx+size]
		copy(to, from)
		*n.valueFlag(int(n.meta.keyCount-1)) = 0
	}
	// do the book keeping
	n.meta.keyCount--
	return nil
}

func loadLeaf(backing []byte) (*leaf, error) {
	n := asLeaf(backing)
	if n.meta.flags&lEAF == 0 {
		return nil, errors.Errorf("Was not a leaf node")
	}
	return n, nil
}

func asLeaf(backing []byte) *leaf {
	back := slice.AsSlice(&backing)
	return (*leaf)(back.Array)
}

func newLeaf(backing []byte, keySize uint16) (*leaf, error) {
	n := asLeaf(backing)

	available := uintptr(len(backing)) - leafMetaSize

	// best case: values are all 1 byte
	// the size value is the size of the valueLength
	//             size + 1 byte
	valMin := uintptr(2 + 1 + minValSize)
	kvSize := uintptr(keySize) + valMin
	keyCap := available / kvSize

	n.meta.Init(lEAF, keySize, uint16(keyCap))
	return n, nil
}

func (n *leaf) release() {
}

