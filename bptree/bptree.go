package bptree

import (
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
	"github.com/timtadh/fs2/varchar"
)

// The Ubiquitous B+ Tree
type BpTree struct {
	bf       *fmap.BlockFile
	metaBack []byte
	meta     *bpTreeMeta
	varchar  *varchar.Varchar
}

type bpTreeMeta struct {
	root        uint64
	itemCount   uint64
	varcharCtrl uint64
	keySize     uint16
	valSize     uint16
	flags       consts.Flag
}

var bpTreeMetaSize uintptr

func init() {
	m := &bpTreeMeta{}
	bpTreeMetaSize = reflect.TypeOf(*m).Size()
}

func newBpTreeMeta(bf *fmap.BlockFile, keySize, valSize uint16, flags consts.Flag) ([]byte, *bpTreeMeta, error) {
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
	b, err := bf.Allocate()
	if err != nil {
		return nil, nil, err
	}
	data := make([]byte, bpTreeMetaSize)
	meta := (*bpTreeMeta)(slice.AsSlice(&data).Array)
	meta.root = a
	meta.keySize = keySize
	meta.valSize = valSize
	meta.varcharCtrl = b
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

// Create a new B+ Tree in the given BlockFile.
//
// bf *BlockFile. Can be an anonymous map or a file backed map
// keySize int. If this is negative it will use varchar keys
// valSize int. If this is negative it will use varchar values
func New(bf *fmap.BlockFile, keySize, valSize int) (*BpTree, error) {
	if bf.BlockSize() != consts.BLOCKSIZE {
		return nil, errors.Errorf("The block size must be %v, got %v", consts.BLOCKSIZE, bf.BlockSize())
	}
	if keysPerInternal(int(bf.BlockSize()), keySize + valSize) < 3 {
		return nil, errors.Errorf("Key is too large (fewer than 3 keys per internal node)")
	}
	if keySize == 0 {
		return nil, errors.Errorf("keySize was 0")
	}
	var flags consts.Flag = 0
	if keySize < 0 {
		keySize = 8
		flags = flags | consts.VARCHAR_KEYS
	}
	if valSize < 0 {
		valSize = 8
		flags = flags | consts.VARCHAR_VALS
	}
	back, meta, err := newBpTreeMeta(bf, uint16(keySize), uint16(valSize), flags)
	if err != nil {
		return nil, err
	}
	v, err := varchar.New(bf, meta.varcharCtrl)
	if err != nil {
		return nil, err
	}
	bpt := &BpTree{
		bf:       bf,
		metaBack: back,
		meta:     meta,
		varchar:  v,
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
	v, err := varchar.Open(bf, meta.varcharCtrl)
	if err != nil {
		return nil, err
	}
	bpt := &BpTree{
		bf:       bf,
		metaBack: back,
		meta:     meta,
		varchar:  v,
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
