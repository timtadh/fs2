package bptree

import (
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

// The Ubiquitous B+ Tree
type BpTree struct {
	bf      *fmap.BlockFile
	varchar *Varchar
	metaOff uint64
	meta    *bpTreeMeta
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

func newBpTreeMeta(bf *fmap.BlockFile, metaOff uint64, keySize, valSize uint16, flags consts.Flag) (*bpTreeMeta, error) {
	a, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	err = bf.Do(a, 1, func(bytes []byte) error {
		_, err := newLeaf(flags, bytes, keySize, valSize)
		return err
	})
	if err != nil {
		return nil, err
	}
	b, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	meta := &bpTreeMeta{
		root:        a,
		itemCount:   0,
		varcharCtrl: b,
		keySize:     keySize,
		valSize:     valSize,
		flags:       flags,
	}
	return meta, nil
}

func loadBpTreeMeta(bf *fmap.BlockFile, metaOff uint64) (meta *bpTreeMeta, err error) {
	err = bf.Do(metaOff, 1, func(data []byte) error {
		m := (*bpTreeMeta)(slice.AsSlice(&data).Array)
		meta = m.Clone()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (m *bpTreeMeta) Clone() *bpTreeMeta {
	return &bpTreeMeta{
		root:        m.root,
		itemCount:   m.itemCount,
		varcharCtrl: m.varcharCtrl,
		keySize:     m.keySize,
		valSize:     m.valSize,
		flags:       m.flags,
	}
}

func (m *bpTreeMeta) CopyInto(o *bpTreeMeta) {
	o.root = m.root
	o.itemCount = m.itemCount
	o.varcharCtrl = m.varcharCtrl
	o.keySize = m.keySize
	o.valSize = m.valSize
	o.flags = m.flags
}

func (b *BpTree) doMeta(do func(*bpTreeMeta) error) error {
	return b.bf.Do(b.metaOff, 1, func(data []byte) error {
		meta := (*bpTreeMeta)(slice.AsSlice(&data).Array)
		return do(meta)
	})
}

func (b *BpTree) writeMeta() error {
	return b.doMeta(func(m *bpTreeMeta) error {
		b.meta.CopyInto(m)
		return nil
	})
}

// Create a new B+ Tree in the given BlockFile.
//
// bf *BlockFile. Can be an anonymous map or a file backed map
// keySize int. If this is negative it will use varchar keys
// valSize int. If this is negative it will use varchar values
func New(bf *fmap.BlockFile, keySize, valSize int) (*BpTree, error) {
	metaOff, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 8)
	moff := slice.AsUint64(&data)
	*moff = metaOff
	err = bf.SetControlData(data)
	if err != nil {
		return nil, err
	}
	return NewAt(bf, metaOff, keySize, valSize)
}

func NewAt(bf *fmap.BlockFile, metaOff uint64, keySize, valSize int) (*BpTree, error) {
	if bf.BlockSize() != consts.BLOCKSIZE {
		return nil, errors.Errorf("The block size must be %v, got %v", consts.BLOCKSIZE, bf.BlockSize())
	}
	if keysPerInternal(int(bf.BlockSize()), keySize+valSize) < 3 {
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
	meta, err := newBpTreeMeta(bf, metaOff, uint16(keySize), uint16(valSize), flags)
	if err != nil {
		return nil, err
	}
	var v *Varchar
	if flags&(consts.VARCHAR_KEYS|consts.VARCHAR_VALS) != 0 {
		v, err = NewVarchar(bf, meta.varcharCtrl)
		if err != nil {
			return nil, err
		}
	}
	bpt := &BpTree{
		bf:      bf,
		metaOff: metaOff,
		meta:    meta,
		varchar: v,
	}
	return bpt, bpt.writeMeta()
}

// Open an existing B+Tree (it knows its key size so you do not have to
// supply that).
func Open(bf *fmap.BlockFile) (*BpTree, error) {
	data, err := bf.ControlData()
	if err != nil {
		return nil, err
	}
	metaOff := *slice.AsUint64(&data)
	return OpenAt(bf, metaOff)
}

func OpenAt(bf *fmap.BlockFile, metaOff uint64) (*BpTree, error) {
	meta, err := loadBpTreeMeta(bf, metaOff)
	if err != nil {
		return nil, err
	}
	var v *Varchar
	if meta.flags&(consts.VARCHAR_KEYS|consts.VARCHAR_VALS) != 0 {
		v, err = OpenVarchar(bf, meta.varcharCtrl)
		if err != nil {
			return nil, err
		}
	}
	bpt := &BpTree{
		bf:      bf,
		metaOff: metaOff,
		meta:    meta,
		varchar: v,
	}
	return bpt, nil
}

// What is the key size of this tree?
func (b *BpTree) KeySize() int {
	return int(b.meta.keySize)
}

// How many items are in the tree?
func (b *BpTree) Size() int {
	return int(b.meta.itemCount)
}
