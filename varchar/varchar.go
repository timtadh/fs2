package varchar

import (
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type Varchar struct {
	bf *fmap.BlockFile
	a uint64
}

type varCtrl struct {
	flags    consts.Flag
	freeLen  uint32
	freeHead uint64
}

const varCtrlSize = 16

type varFree struct {
	flags consts.Flag
	length uint32
	prev uint64
	next uint64
}

const varFreeSize = 24

const mAX_UINT32 uint32 = 0xffffffff

type varRunMeta struct {
	flags  consts.Flag
	length uint32
	refs   uint32
}

const varRunMetaSize = 12

type varRun struct {
	meta  varRunMeta
	bytes [mAX_UINT32-varRunMetaSize]byte
}

func assert_len(bytes []byte, length int) {
	if length > len(bytes) {
		panic(errors.Errorf("Expected byte slice to be at least %v bytes long but was %v", length, len(bytes)))
	}
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
	vc.flags = consts.VARCHAR_CTRL
	vc.freeLen = 0
	vc.freeHead = 0
}

// Create a new varchar structure. This takes a blockfile and an offset
// of an allocated block. The block becomes the control block for the
// varchar file (storing the free list for the allocator). It is
// important for the parent structure to track the location of this
// control block.
func New(bf *fmap.BlockFile, a uint64) (v *Varchar, err error) {
	v = &Varchar{bf: bf, a: a}
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

// Open a varchar structure in the given blockfile with the given offset
// as the control block. This function will confirm that the control
// block is indeed a properly formated control block.
func Open(bf *fmap.BlockFile, a uint64) (v *Varchar, err error) {
	v = &Varchar{bf: bf, a: a}
	err = v.bf.Do(v.a, 1, func(bytes []byte) error {
		ctrl := asCtrl(bytes)
		if ctrl.flags&consts.VARCHAR_CTRL == 0 {
			return errors.Errorf("Expected a Varchar control block")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Varchar) Alloc(length int) (a uint64, err error) {
	return 0, errors.Errorf("Unimplemented")
}

func (v *Varhar) Free(a uint64) (err error) {
	return errors.Errorf("Unimplemented")
}

func (v *Varchar) Do(a uint64, do func(bytes []byte) error) (err error) {
	return errors.Errorf("Unimplemented")
}

func (v *Varchar) Ref(a uint64) (err error) {
	return errors.Errorf("Unimplemented")
}

func (v *Varchar) Deref(a uint64) (err error) {
	return errors.Errorf("Unimplemented")
}

func asCtrl(backing []byte) *varCtrl {
	assert_len(backing, varCtrlSize)
	back := slice.AsSlice(&backing)
	return (*varCtrl)(back.Array)
}

func asFree(backing []byte) *varFree {
	assert_len(backing, varFreeSize)
	back := slice.AsSlice(&backing)
	return (*varFree)(back.Array)
}

func asRun(backing []byte) *varRun {
	assert_len(backing, varRunMetaSize)
	back := slice.AsSlice(&backing)
	return (*varRun)(back.Array)
}

func asRunMeta(backing []byte) *varRunMeta {
	assert_len(backing, varRunMetaSize)
	back := slice.AsSlice(&backing)
	return (*varRunMeta)(back.Array)
}

// TIM YOU ARE HERE
// ADDING VAR RUNS TO THIS METHOD
// fixing the fact that these could be unaligned block access
// You should load the amount of bytes need for the struct. This could be
// crossing a block boundry. However, these guys are small. So we could
// a) force them on creation to always be block aligned.
// b) load up 2 blocks (there can never be 3)
func (v *Varchar) do(
	a uint64,
	ctrlDo func(*varCtrl) error,
	freeDo func(*varFree) error,
	runDo func(*varRunMeta) error,
) error {
	blkSize := uint64(v.bf.BlockSize())
	offset := a % blkSize
	start := a - offset
	var blks uint64 = 1
	if offset + varFreeSize > blkSize {
		blks = 2
	}
	return v.bf.Do(start, blks, func(bytes []byte) error {
		bytes = bytes[offset:]
		flags := consts.Flag(bytes[0])
		if flags&consts.VARCHAR_CTRL != 0 {
			return ctrlDo(asCtrl(bytes))
		} else if flags&consts.VARCHAR_FREE != 0 {
			return freeDo(asFree(bytes))
		} else if flags&consts.VARCHAR_RUN != 0 {
			return runDo(asRunMeta(bytes))
		} else {
			return errors.Errorf("Unknown block type")
		}
	})
}
