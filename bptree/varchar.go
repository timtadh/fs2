package bptree

import (
	"reflect"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type varchar struct {
	bf *fmap.BlockFile
	a uint64
}

type varCtrl struct {
	flags    Flag
	freeLen  uint32
	freeHead uint64
}

const varCtrlSize = 16

type varFree struct {
	flags Flag
	length uint32
	prev uint64
	next uint64
}

const varFreeSize = 24

const mAX_UINT32 uint32 = 0xffffffff

type varRunMeta struct {
	flags  Flag
	length uint32
	refs   uint32
}

const varRunMetaSize = 12

type varRun struct {
	meta  varRunMeta
	bytes [mAX_UINT32-varRunMetaSize]byte
}

func init() {
	var vc varCtrl
	var vf varFree
	var vr varRunMeta
	vc_size := reflect.TypeOf(vc).Size()
	vf_size := reflect.TypeOf(vf).Size()
	vr_size := reflect.TypeOf(vr).Size()
	if vc_size != varCtrlSize {
		panic("the varCtrl was an unexpected size")
	}
	if vf_size != varFreeSize {
		panic("the varFree was an unexpected size")
	}
	if vr_size != varRunMetaSize {
		panic("the varFree was an unexpected size")
	}
}

func (vc *varCtrl) Init() {
	vc.flags = vARCHAR_CTRL
	vc.freeLen = 0
	vc.freeHead = 0
}

func newVarchar(bf *fmap.BlockFile, a uint64) (v *varchar, err error) {
	v = &varchar{bf: bf, a: a}
	err = v.bf.Do(v.a, 1, func(bytes []byte) error {
		ctrl := asCtrl(bytes)
		ctrl.Init()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func loadVarchar(bf *fmap.BlockFile, a uint64) (v *varchar, err error) {
	v = &varchar{bf: bf, a: a}
	err = v.bf.Do(v.a, 1, func(bytes []byte) error {
		ctrl := asCtrl(bytes)
		if ctrl.flags&vARCHAR_CTRL == 0 {
			return errors.Errorf("Expected a varchar control block")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func blksNeeded(bf *fmap.BlockFile, size int) int {
	blk := int(bf.BlockSize())
	m := size % blk
	if m == 0 {
		return size / blk
	}
	return (size + (blk - m)) / blk
}

func blksNeeded(bf *fmap.BlockFile, size int) int {
	blk := int(bf.BlockSize())
	m := size % blk
	if m == 0 {
		return size / blk
	}
	return (size + (blk - m)) / blk
}

func asCtrl(backing []byte) *varCtrl {
	back := slice.AsSlice(&backing)
	return (*varCtrl)(back.Array)
}

func asFree(backing []byte) *varFree {
	back := slice.AsSlice(&backing)
	return (*varFree)(back.Array)
}

func asRun(backing []byte) *varFree {
	back := slice.AsSlice(&backing)
	return (*varRun)(back.Array)
}

func (v *varchar) do(
	a uint64,
	ctrlDo func(*varCtrl) error,
	freeDo func(*varFree) error,
) error {
	return v.bf.Do(a, 1, func(bytes []byte) error {
		flags := Flag(bytes[0])
		if flags&vARCHAR_CTRL != 0 {
			return ctrlDo(asCtrl(bytes))
		} else if flags&vARCHAR_FREE != 0 {
			return freeDo(asFree(bytes))
		} else {
			return errors.Errorf("Unknown block type")
		}
	})
}
