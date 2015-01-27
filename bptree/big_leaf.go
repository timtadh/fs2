package bptree

import (
	"fmt"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)


type bigLeafMeta struct {
	leafMeta
	valueSize uint32
}

type bigLeaf struct {
	back []byte
	meta *bigLeafMeta
	key []byte
	value []byte
}

func loadBigLeafMeta(backing []byte) *bigLeafMeta {
	back := slice.AsSlice(&backing)
	return (*bigLeafMeta)(back.Array)
}

func (m *bigLeafMeta) Init(flags flag, keySize, keyCap uint16, valueSize uint32) {
	m.leafMeta.Init(flags, keySize, keyCap)
	m.valueSize = valueSize
	m.keyCount = 1
}

func (m *bigLeafMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *bigLeafMeta) String() string {
	return fmt.Sprintf(
		"%v, valueSize: %v",
		&m.leafMeta, m.valueSize)
}

func (n *bigLeaf) String() string {
	return fmt.Sprintf(
		"meta: <%v>, key: %v, value: %v",
		n.meta, n.key, n.value)
}

func (n *bigLeaf) setKey(key []byte) error {
	if len(key) != len(n.key) {
		return fmt.Errorf("key was the wrong size")
	}
	copy(n.key, key)
	return nil
}

func (n *bigLeaf) setValue(value []byte) error {
	if len(value) != len(n.value) {
		return fmt.Errorf("value was the wrong size")
	}
	copy(n.value, value)
	return nil
}

func loadBigLeaf(backing []byte) (*bigLeaf, error) {
	meta := loadBigLeafMeta(backing)
	if meta.flags & BIG_LEAF == 0 {
		return nil, fmt.Errorf("Was not a big leaf node")
	}
	return attachBigLeaf(backing, meta)
}

func newBigLeaf(backing []byte, keySize uint16, valueSize uint32) (*bigLeaf, error) {
	meta := loadBigLeafMeta(backing)
	if len(backing) < int(meta.Size()) + int(keySize) + int(valueSize) {
		return nil, fmt.Errorf("backing array not large enough")
	}
	meta.Init(BIG_LEAF, keySize, 1, valueSize)
	return attachBigLeaf(backing, meta)
}

func attachBigLeaf(backing []byte, meta *bigLeafMeta) (*bigLeaf, error) {
	back := slice.AsSlice(&backing)
	ptr := uintptr(back.Array) + meta.Size()
	end := uintptr(back.Array) + uintptr(back.Cap)

	key_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: int(meta.keySize),
		Cap: int(meta.keySize),
	}
	key := *key_s.AsBytes()
	ptr += uintptr(meta.keySize)

	value_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: int(meta.valueSize),
		Cap: int(meta.valueSize),
	}
	value := *value_s.AsBytes()
	ptr += uintptr(meta.valueSize)

	if ptr > end {
		return nil, fmt.Errorf("(overflow) backing array not large enough")
	}

	return &bigLeaf{
		back: backing,
		meta: meta,
		key: key,
		value: value,
	}, nil
}

