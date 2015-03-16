package bptree

import (
	"reflect"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

// The Ubiquitous B+ Tree
type BpTree struct {
	bf            *fmap.BlockFile
	metaBack      []byte
	meta          *bpTreeMeta
}

type bpTreeMeta struct {
	root    uint64
	itemCount uint64
	keySize uint16
	valSize uint16
	flags Flag
}

var bpTreeMetaSize uintptr

func init() {
	m := &bpTreeMeta{}
	bpTreeMetaSize = reflect.TypeOf(*m).Size()
}

func newBpTreeMeta(bf *fmap.BlockFile, keySize, valSize uint16, flags Flag) ([]byte, *bpTreeMeta, error) {
	a, err := bf.Allocate()
	if err != nil {
		return nil, nil, err
	}
	err = bf.Do(a, 1, func(bytes []byte) error {
		_, err := newLeaf(flags, bytes, keySize, valSize)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	data := make([]byte, bpTreeMetaSize)
	meta := (*bpTreeMeta)(slice.AsSlice(&data).Array)
	meta.root = a
	meta.keySize = keySize
	meta.itemCount = 0
	meta.flags = flags
	err = bf.SetControlData(data)
	if err != nil {
		return nil, nil, err
	}
	return data, meta, nil
}

func loadBpTreeMeta(bf *fmap.BlockFile) ([]byte, *bpTreeMeta, error) {
	data, err := bf.ControlData()
	if err != nil {
		return nil, nil, err
	}
	meta := (*bpTreeMeta)(slice.AsSlice(&data).Array)
	if meta.root == 0 || meta.keySize == 0 {
		return nil, nil, errors.Errorf("Meta was not properly initialized. Can't load tree")
	}
	return data, meta, nil
}

// Create a new B+ Tree in the given BlockFile with the given key size
// (in bytes).  The size of the key cannot change after creation. The
// maximum size is about ~1350 bytes.
func New(bf *fmap.BlockFile, keySize, valSize int) (*BpTree, error) {
	if bf.BlockSize() != BLOCKSIZE {
		return nil, errors.Errorf("The block size must be %v, got %v", BLOCKSIZE, bf.BlockSize())
	}
	if keysPerInternal(int(bf.BlockSize()), keySize) < 3 {
		return nil, errors.Errorf("Key is too large (fewer than 3 keys per internal node)")
	}
	back, meta, err := newBpTreeMeta(bf, uint16(keySize), uint16(valSize), 0)
	if err != nil {
		return nil, err
	}
	bpt := &BpTree{
		bf:            bf,
		metaBack:      back,
		meta:          meta,
	}
	return bpt, nil
}

// Open an existing B+Tree (it knows its key size so you do not have to
// supply that).
func Open(bf *fmap.BlockFile) (*BpTree, error) {
	back, meta, err := loadBpTreeMeta(bf)
	if err != nil {
		return nil, err
	}
	bpt := &BpTree{
		bf:            bf,
		metaBack:      back,
		meta:          meta,
	}
	return bpt, nil
}

func (b *BpTree) writeMeta() error {
	return b.bf.SetControlDataNoSync(b.metaBack)
}

// What is the key size of this tree?
func (self *BpTree) KeySize() int {
	return int(self.meta.keySize)
}

// How many items are in the tree?
func (self *BpTree) Size() int {
	return int(self.meta.itemCount)
}

