package mmlist

import (
	"encoding/binary"
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type List struct {
	bf *fmap.BlockFile
	varchar *bptree.Varchar
	idxTree *bptree.BpTree
	a uint64
	blkSize int
}

type ctrlBlk struct {
	flags    consts.Flag
	varchar  uint64
	idxTree  uint64
	count    uint64
}

const ctrlBlkSize = 32

type idxBlk struct {
	flags consts.Flag
	count uint16
	items [(consts.BLOCKSIZE/8)-1]uint64
}

const idxBlkSize = consts.BLOCKSIZE

func init() {
	var c ctrlBlk
	var i idxBlk
	c_size := reflect.TypeOf(c).Size()
	i_size := reflect.TypeOf(i).Size()
	if c_size != ctrlBlkSize {
		panic("the ctrlBlk was an unexpected size")
	}
	if i_size != idxBlkSize {
		panic("the idxBlk was an unexpected size")
	}
}

func assert_len(bytes []byte, length int) {
	if length > len(bytes) {
		panic(errors.Errorf("Expected byte slice to be at least %v bytes long but was %v", length, len(bytes)))
	}
}

func (c *ctrlBlk) Init(varchar, idxTree uint64) {
	c.flags = consts.LIST_CTRL
	c.varchar = varchar
	c.idxTree = idxTree
	c.count = 0
}

func (b *idxBlk) Init() {
	b.flags = consts.LIST_IDX
	b.count = 0
	for i := range b.items {
		b.items[i] = 0
	}
}

func (b *idxBlk) Append(a uint64) (error) {
	if b.count + 1 > uint16(len(b.items)) {
		return errors.Errorf("Could not append to idxBlk, blk full")
	}
	b.items[b.count] = a
	b.count++
	return nil
}

func (b *idxBlk) Pop() (uint64, error) {
	if b.count == 0 {
		return 0, errors.Errorf("Could not pop from idx, blk empty")
	}
	b.count--
	a := b.items[b.count]
	b.items[b.count] = 0
	return a, nil
}

func (b *idxBlk) Get(i int) (uint64, error) {
	if uint16(i) < 0 || uint16(i) >= b.count {
		return 0, errors.Errorf("Idx out of range for Get")
	}
	return b.items[i], nil
}

func (b *idxBlk) Set(i int, a uint64) (error) {
	if uint16(i) < 0 || uint16(i) >= b.count {
		return errors.Errorf("Idx out of range for Set")
	}
	b.items[i] = a
	return nil
}

func New(bf *fmap.BlockFile) (*List, error) {
	panic("halp")
}

func NewAt(bf *fmap.BlockFile, a uint64) (*List, error) {
	panic("halp")
}

func Open(bf *fmap.BlockFile) (*List, error) {
	panic("halp")
}

func OpenAt(bf *fmap.BlockFile, a uint64) (*List, error) {
	panic("halp")
}

func (l *List) Size() int {
	panic("halp")
}

func (l *List) Append(item []byte) (int, error) {
	panic("halp")
}

func (l *List) Pop() ([]byte, error) {
	panic("halp")
}

func (l *List) Get(i int) ([]byte, error) {
	panic("halp")
}

func (l *List) Set(i int, item []byte) (error) {
	panic("halp")
}

func (l *List) idxKey(i int) (key []byte) {
	idx := uint64(i >> 3)
	key = make([]byte, 8)
	binary.LittleEndian.PutUint64(key, idx)
	return key
}

func (l *List) asCtrl(bytes []byte) *ctrlBlk {
	assert_len(bytes, ctrlBlkSize)
	back := slice.AsSlice(&bytes)
	return (*ctrlBlk)(back.Array)
}

func (l *List) asIdx(bytes []byte) *idxBlk {
	assert_len(bytes, ctrlBlkSize)
	back := slice.AsSlice(&bytes)
	return (*idxBlk)(back.Array)
}

func (l *List) do(
	a uint64,
	ctrlDo func(*ctrlBlk) error,
	idxDo func(*idxBlk) error,
) error {
	return l.bf.Do(a, 1, func(bytes []byte) error {
		flags := consts.AsFlag(bytes)
		if flags == consts.LIST_CTRL {
			return ctrlDo(l.asCtrl(bytes))
		} else if flags == consts.LIST_IDX {
			return idxDo(l.asIdx(bytes))
		} else {
			return errors.Errorf("Unknown block type, %v", flags)
		}
	})
}

