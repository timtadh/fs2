// A Memory Mapped List. This list works more like a stack and less like a
// queue.  It is not a good thing to build a job queue on. It is a good thing to
// build a large set of items which can be efficiently randomly sampled. It uses
// the same `varchar` system that the B+Tree uses so it can store variably sized
// items up to 2^31 - 1 bytes long.
// 
// Operations
// 
// 1. `Size` O(1)
// 2. `Append` O(1)
// 3. `Pop` O(1)
// 4. `Get` O(1)
// 5. `Set` O(1)
//
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
	count uint64
}

type ctrlBlk struct {
	flags    consts.Flag
	varchar  uint64
	idxTree  uint64
	count    uint64
}

const ctrlBlkSize = 32

const itemsPerIdx = (consts.BLOCKSIZE/8)-1

type idxBlk struct {
	flags consts.Flag
	count uint16
	items [itemsPerIdx]uint64
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

func (b *idxBlk) Get(i uint16) (uint64, error) {
	if i < 0 || i >= b.count {
		return 0, errors.Errorf("Idx out of range for Get")
	}
	return b.items[i], nil
}

func (b *idxBlk) Set(i uint16, a uint64) (error) {
	if i < 0 || i >= b.count {
		return errors.Errorf("Idx out of range for Set")
	}
	b.items[i] = a
	return nil
}

func New(bf *fmap.BlockFile) (*List, error) {
	ctrl_a, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 8)
	moff := slice.AsUint64(&data)
	*moff = ctrl_a
	err = bf.SetControlData(data)
	if err != nil {
		return nil, err
	}
	return NewAt(bf, ctrl_a)
}

func NewAt(bf *fmap.BlockFile, ctrl_a uint64) (*List, error) {
	vc_a, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	it_a, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	v, err := bptree.NewVarchar(bf, vc_a)
	if err != nil {
		return nil, err
	}
	it, err := bptree.NewAt(bf, it_a, 8, 8)
	if err != nil {
		return nil, err
	}
	l := &List{
		bf: bf,
		varchar: v,
		idxTree: it,
		a: ctrl_a,
		count: 0,
	}
	err = l.bf.Do(ctrl_a, 1, func(bytes []byte) error {
		c := l.asCtrl(bytes)
		c.Init(vc_a, it_a)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func Open(bf *fmap.BlockFile) (*List, error) {
	data, err := bf.ControlData()
	if err != nil {
		return nil, err
	}
	ctrl_a := *slice.AsUint64(&data)
	return OpenAt(bf, ctrl_a)
}

func OpenAt(bf *fmap.BlockFile, ctrl_a uint64) (*List, error) {
	l := &List{bf: bf, a: ctrl_a}
	err := l.doCtrl(l.a, func(ctrl *ctrlBlk) (err error) {
		l.varchar, err = bptree.OpenVarchar(bf, ctrl.varchar)
		if err != nil {
			return err
		}
		l.idxTree, err = bptree.OpenAt(bf, ctrl.idxTree)
		if err != nil {
			return err
		}
		l.count = ctrl.count
		return nil
	})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *List) Size() uint64 {
	return l.count
}

func (l *List) Append(item []byte) (i uint64, err error) {
	a, err := l.varchar.Alloc(len(item))
	if err != nil {
		return 0, err
	}
	err = l.varchar.Do(a, func(data []byte) error {
		copy(data, item)
		return nil
	})
	if err != nil {
		return 0, err
	}
	err = l.nextBlk(func(idx *idxBlk) (err error) {
		return idx.Append(a)
	})
	if err != nil {
		return 0, err
	}
	err = l.doCtrl(l.a, func(ctrl *ctrlBlk) error {
		i = ctrl.count
		ctrl.count++
		l.count = ctrl.count
		return nil
	})
	if err != nil {
		return 0, err
	}
	return i, nil
}

func (l *List) Pop() (item []byte, err error) {
	if l.count == 0 {
		return nil, errors.Errorf("Cannot pop an empty list")
	}
	var a uint64
	err = l.lastBlk(func(idx *idxBlk) (err error) {
		a, err = idx.Pop()
		return err
	})
	if err != nil {
		return nil, err
	}
	err = l.varchar.Do(a, func(data []byte) error {
		item = make([]byte, len(data))
		copy(item, data)
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = l.varchar.Free(a)
	if err != nil {
		return nil, err
	}
	err = l.doCtrl(l.a, func(ctrl *ctrlBlk) error {
		ctrl.count--
		l.count = ctrl.count
		return nil
	})
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (l *List) Get(i uint64) (item []byte, err error) {
	if i >= l.count {
		return nil, errors.Errorf("index out of range")
	}
	err = l.blk(uint64(i), func(idx *idxBlk) error {
		a, err := idx.Get(uint16(i % itemsPerIdx))
		if err != nil {
			return err
		}
		return l.varchar.Do(a, func(data []byte) error {
			item = make([]byte, len(data))
			copy(item, data)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (l *List) Set(i uint64, item []byte) (err error) {
	if i >= l.count {
		return errors.Errorf("index out of range")
	}
	var old_a uint64
	err = l.blk(uint64(i), func(idx *idxBlk) (err error) {
		old_a, err = idx.Get(uint16(i % itemsPerIdx))
		return err
	})
	if err != nil {
		return err
	}
	err = l.varchar.Free(old_a)
	if err != nil {
		return err
	}
	a, err := l.varchar.Alloc(len(item))
	if err != nil {
		return err
	}
	err = l.varchar.Do(a, func(data []byte) error {
		copy(data, item)
		return nil
	})
	if err != nil {
		return err
	}
	return l.blk(uint64(i), func(idx *idxBlk) (err error) {
		err = idx.Set(uint16(i % itemsPerIdx), a)
		if err != nil {
			return err
		}
		return 
	})
}

func (l *List) idxKey(i uint64) (key []byte) {
	idx := i / itemsPerIdx
	key = make([]byte, 8)
	binary.LittleEndian.PutUint64(key, idx)
	return key
}

func (l *List) lastBlk(do func(*idxBlk) error) error {
	if l.count < itemsPerIdx {
		return l.blk(0, do)
	} else if l.count % itemsPerIdx == 0 {
		return l.blk(l.count - 1, do)
	} else {
		return l.blk(l.count, do)
	}
}

func (l *List) nextBlk(do func(*idxBlk) error) error {
	key := l.idxKey(l.count)
	if has, err := l.idxTree.Has(key); err != nil {
		return err
	} else if !has {
		a, err := l.newBlk(key)
		if err != nil {
			return err
		}
		return l.doIdx(a, do)
	} else {
		return l.blk(l.count, do)
	}
}

func (l *List) blk(i uint64, do func(*idxBlk) error) error {
	key := l.idxKey(i)
	return l.idxTree.DoFind(key, func(_, value []byte) error {
		a := *slice.AsUint64(&value)
		return l.doIdx(a, do)
	})
}

func (l *List) newBlk(key []byte) (uint64, error) {
	a, err := l.bf.Allocate()
	if err != nil {
		return 0, err
	}
	err = l.idxTree.Add(key, slice.Uint64AsSlice(&a))
	if err != nil {
		return 0, err
	}
	err = l.bf.Do(a, 1, func(bytes []byte) error {
		blk := l.asIdx(bytes)
		blk.Init()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return a, nil
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

func (l *List) doCtrl(a uint64, do func(*ctrlBlk) error) error {
	return l.do(a, do, func(_ *idxBlk) error {
		return errors.Errorf("Unexpected idxBlk")
	})
}

func (l *List) doIdx(a uint64, do func(*idxBlk) error) error {
	return l.do(a, func(_ *ctrlBlk) error {
		return errors.Errorf("Unexpected ctrlBlk")
	}, do)
}

func (l *List) do(
	a uint64,
	doCtrl func(*ctrlBlk) error,
	doIdx func(*idxBlk) error,
) error {
	return l.bf.Do(a, 1, func(bytes []byte) error {
		flags := consts.AsFlag(bytes)
		if flags == consts.LIST_CTRL {
			return doCtrl(l.asCtrl(bytes))
		} else if flags == consts.LIST_IDX {
			return doIdx(l.asIdx(bytes))
		} else {
			return errors.Errorf("Unknown block type, %v at %v", flags, a)
		}
	})
}

