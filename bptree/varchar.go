package bptree

import (
	"encoding/binary"
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type Varchar struct {
	bf       *fmap.BlockFile
	posTree  *BpTree
	sizeTree *BpTree
	a        uint64
	blkSize  int
}

type varCtrl struct {
	flags    consts.Flag
	posTree  uint64
	sizeTree uint64
}

const varCtrlSize = 24

type listNode struct {
	prev uint64
	next uint64
}

type nodeDoer func(uint64, func(*listNode) error) error

type varFree struct {
	flags  consts.Flag
	length uint32
	list   listNode
}

const varFreeSize = 24

const maxArraySize uint32 = 0x7fffffff

type varRunMeta struct {
	flags  consts.Flag
	length uint32
	extra  uint32
	refs   uint32
}

const varRunMetaSize = 16

type varRun struct {
	meta  varRunMeta
	bytes [maxArraySize]byte
}

func assert_len(bytes []byte, length int) {
	if length > len(bytes) {
		panic(errors.Errorf("Expected byte slice to be at least %v bytes long but was %v", length, len(bytes)))
	}
}

const minChunkSize = 128

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

func (vc *varCtrl) Init(posTree, sizeTree uint64) {
	vc.flags = consts.VARCHAR_CTRL
	vc.posTree = posTree
	vc.sizeTree = sizeTree
}

func (vrm *varRunMeta) Init(length, extra int) {
	vrm.flags = consts.VARCHAR_RUN
	vrm.length = uint32(length)
	vrm.extra = uint32(extra)
	vrm.refs = 1
}

func makeBKey(key uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, key)
	return k
}

func makeBSize(size int) []byte {
	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k, uint32(size))
	return k
}

func makeKey(bkey []byte) uint64 {
	return binary.BigEndian.Uint64(bkey)
}

func makeSize(bsize []byte) int {
	return int(binary.BigEndian.Uint32(bsize))
}

// Create a new varchar structure. This takes a blockfile and an offset
// of an allocated block. The block becomes the control block for the
// varchar file (storing the free list for the allocator). It is
// important for the parent structure to track the location of this
// control block.
func NewVarchar(bf *fmap.BlockFile, a uint64) (v *Varchar, err error) {
	ptOff, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	posTree, err := NewAt(bf, ptOff, 8, 0)
	if err != nil {
		return nil, err
	}
	szOff, err := bf.Allocate()
	if err != nil {
		return nil, err
	}
	sizeTree, err := NewAt(bf, szOff, 4, 8)
	if err != nil {
		return nil, err
	}
	v = &Varchar{
		bf:       bf,
		posTree:  posTree,
		sizeTree: sizeTree,
		a:        a,
		blkSize:  bf.BlockSize(),
	}
	err = v.bf.Do(v.a, 1, func(bytes []byte) error {
		ctrl := asCtrl(bytes)
		ctrl.Init(ptOff, szOff)
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
func OpenVarchar(bf *fmap.BlockFile, a uint64) (v *Varchar, err error) {
	v = &Varchar{bf: bf, a: a, blkSize: bf.BlockSize()}
	var ptOff uint64
	var szOff uint64
	err = v.bf.Do(v.a, 1, func(bytes []byte) error {
		ctrl := asCtrl(bytes)
		if ctrl.flags&consts.VARCHAR_CTRL == 0 {
			return errors.Errorf("Expected a Varchar control block")
		}
		ptOff = ctrl.posTree
		szOff = ctrl.sizeTree
		return nil
	})
	if err != nil {
		return nil, err
	}
	v.posTree, err = OpenAt(bf, ptOff)
	if err != nil {
		return nil, err
	}
	v.sizeTree, err = OpenAt(bf, szOff)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Allocate a varchar of the desired length.
func (v *Varchar) Alloc(length int) (a uint64, err error) {
	if uint32(length) >= maxArraySize {
		return 0, errors.Errorf("Size is too large. Cannon allocate an array that big")
	}
	fullLength := v.allocAmt(length)
	next, err := v.sizeTree.UnsafeRange(makeBSize(fullLength), nil)
	if err != nil {
		return 0, err
	}
	var bkey []byte
	var bsize []byte
	var found = false
	var a_length int
	for bsize, bkey, err, next = next(); next != nil; bsize, bkey, err, next = next() {
		size := makeSize(bsize)
		if fullLength < size {
			found = true
			a = makeKey(bkey)
			a_length = size
			break
		}
	}
	if err != nil {
		return 0, err
	}
	if !found {
		return v.allocNew(length, fullLength)
	}
	err = v.indexRemove(a_length, a)
	if err != nil {
		return 0, err
	}
	err = v.newRun(a, length, fullLength, a_length)
	if err != nil {
		return 0, err
	}
	return a, nil
}

func (v *Varchar) allocNew(length, fullLength int) (a uint64, err error) {
	blks := v.blksNeeded(fullLength)
	if blks > 1 {
		a, err = v.bf.AllocateBlocks(blks)
	} else {
		a, err = v.bf.Allocate()
	}
	if err != nil {
		return 0, err
	}
	err = v.free(a, blks*int(v.bf.BlockSize()))
	if err != nil {
		return 0, err
	}
	return v.Alloc(length)
}

func (v *Varchar) allocAmt(length int) int {
	fullLength := length + varRunMetaSize
	if fullLength < varFreeSize {
		return varFreeSize
	}
	return fullLength
}

// This frees the extra on the end of the segment starting at `a` for
// length `aLen`. If the extra is too short it will return the amount of
// extra. This should be added to the meta data for the attached run.
func (v *Varchar) freeExtra(a uint64, aLen, allocLen int) (extra int, err error) {
	if aLen == allocLen {
		return 0, nil
	} else if aLen < allocLen {
		return 0, errors.Errorf("underallocated !")
	}
	freeSize := aLen - allocLen
	if freeSize < varFreeSize {
		return freeSize, nil
	}
	e := a + uint64(allocLen)
	err = v.free(e, freeSize)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (v *Varchar) blksNeeded(length int) int {
	blkSize := int(v.bf.BlockSize())
	m := length % blkSize
	if m == 0 {
		return length / blkSize
	}
	return (length + (blkSize - m)) / blkSize
}

// Free the varchar at the address a.
func (v *Varchar) Free(a uint64) (err error) {
	var length int
	err = v.doRun(a, func(m *varRunMeta) error {
		length = int(m.length+m.extra) + varRunMetaSize
		return nil
	})
	if err != nil {
		return err
	}
	return v.free(a, length)
}

// free block starting at a running for length.
func (v *Varchar) free(a uint64, length int) (err error) {
	err = v.newFree(a, length)
	if err != nil {
		return err
	}
	blk, i, err := v.posTree.getStart(makeBKey(a))
	if err != nil {
		return err
	}
	var bnext_a []byte = make([]byte, 8)
	err = v.posTree.doLeaf(blk, func(n *leaf) error {
		copy(bnext_a, n.key(i))
		return nil
	})
	if err != nil {
		return err
	}
	next_a := makeKey(bnext_a)
	var prev_a uint64
	if next_a < a {
		prev_a = next_a
	} else {
		var bprev_a []byte = make([]byte, 8)
		pblk, pi, end, err := v.posTree.prevLoc(blk, i)
		if err != nil {
			return err
		}
		if !end {
			err = v.posTree.doLeaf(pblk, func(n *leaf) error {
				copy(bprev_a, n.key(pi))
				return nil
			})
			if err != nil {
				return err
			}
		}
		prev_a = makeKey(bprev_a)
	}
	if next_a > a && a+uint64(length) == next_a {
		var next_length int
		err = v.doFree(a, func(cur *varFree) error {
			return v.doFree(next_a, func(next *varFree) error {
				next_length = int(next.length)
				cur.length += next.length
				next.length = 0
				next.flags = 0xff
				return nil
			})
		})
		if err != nil {
			return err
		}
		err = v.indexRemove(next_length, next_a)
		if err != nil {
			return err
		}
	}
	var mustInsert bool = true
	if prev_a != 0 {
		var prev_length int
		var new_length int
		err = v.doFree(prev_a, func(prev *varFree) error {
			if prev_a+uint64(prev.length) == a {
				mustInsert = false
				return v.doFree(a, func(cur *varFree) error {
					prev_length = int(prev.length)
					prev.length += cur.length
					new_length = int(prev.length)
					cur.length = 0
					cur.flags = 0xff
					return nil
				})
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !mustInsert {
			err = v.indexRemove(prev_length, prev_a)
			if err != nil {
				return err
			}
			return v.indexAdd(new_length, prev_a)
		}
	}
	if mustInsert {
		var a_length int
		err = v.doFree(a, func(cur *varFree) error {
			a_length = int(cur.length)
			return nil
		})
		if err != nil {
			return err
		}
		return v.indexAdd(a_length, a)
	}
	return nil
}

func (v *Varchar) szAdd(length int, a uint64) error {
	return v.sizeTree.Add(makeBSize(length), makeBKey(a))
}

func (v *Varchar) szDel(length int, a uint64) error {
	has, err := v.sizeTree.Has(makeBSize(length))
	if err != nil {
		return err
	} else if !has {
		return nil
	}
	return v.sizeTree.Remove(makeBSize(length), func(bytes []byte) bool {
		return a == makeKey(bytes)
	})
}

func (v *Varchar) indexAdd(length int, a uint64) error {
	err := v.szAdd(length, a)
	if err != nil {
		return err
	}
	return v.posTree.Add(makeBKey(a), make([]byte, 0))
}

func (v *Varchar) indexRemove(length int, a uint64) error {
	err := v.szDel(length, a)
	if err != nil {
		return err
	}
	has, err := v.posTree.Has(makeBKey(a))
	if err != nil {
		return err
	} else if !has {
		return nil
	}
	return v.posTree.Remove(makeBKey(a), func(_ []byte) bool { return true })
}

// Interact with the contents of the varchar. The bytes passed into the
// callback are UNSAFE. You could cause a segmentation fault if you
// simply copy the *slice* out of the function. You need to copy the
// data instead.
//
// The right way:
//
// 	var myBytes []byte
// 	err = v.Do(a, func(bytes []byte) error {
// 		myBytes = make([]byte, len(bytes))
// 		copy(myBytes, bytes)
// 		return nil
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
//
// you can of course interact with the bytes in the callback in any way
// you want as long as no pointers escape. You can even change the
// values of the bytes (and these changes will be persisted). However,
// you cannot change the length of the varchar.
func (v *Varchar) Do(a uint64, do func([]byte) error) (err error) {
	return v.doRun(a, func(m *varRunMeta) error {
		fullLength := v.allocAmt(int(m.length))
		blks := uint64(v.blksNeeded(fullLength))
		offset, start, _ := v.startOffsetBlks(a)
		for offset+uint64(fullLength) >= blks*uint64(v.bf.BlockSize()) {
			blks++
		}
		size, err := v.bf.Size()
		if err != nil {
			return err
		}
		for start+blks*uint64(v.bf.BlockSize()) > uint64(size) {
			blks--
		}
		return v.bf.Do(start, blks, func(bytes []byte) error {
			bytes = bytes[offset:]
			flags := consts.AsFlag(bytes)
			if flags&consts.VARCHAR_RUN == 0 {
				return errors.Errorf("bad address, was not a run block")
			}
			r := asRun(bytes)
			return do(r.bytes[:r.meta.length])
		})
	})
}

func (v *Varchar) UnsafeGet(a uint64) (bytes []byte, err error) {
	rbytes, err := v.unsafeGet(a)
	if err != nil {
		return nil, err
	}
	m := asRunMeta(rbytes)
	fullLength := v.allocAmt(int(m.length))
	blks := uint64(v.blksNeeded(fullLength))
	offset, start, _ := v.startOffsetBlks(a)
	for offset+uint64(fullLength) >= blks*uint64(v.bf.BlockSize()) {
		blks++
	}
	size, err := v.bf.Size()
	if err != nil {
		return nil, err
	}
	for start+blks*uint64(v.bf.BlockSize()) > uint64(size) {
		blks--
	}
	allBytes, err := v.bf.Get(start, blks)
	if err != nil {
		return nil, err
	}
	err = v.bf.Release(allBytes)
	if err != nil {
		return nil, err
	}
	bytes = allBytes[offset:]
	flags := consts.AsFlag(bytes)
	if flags&consts.VARCHAR_RUN == 0 {
		return nil, errors.Errorf("bad address, was not a run block")
	}
	r := asRun(bytes)
	return r.bytes[:r.meta.length], nil
}

// Ref increments the ref field of the block. It starts out as one (when
// allocated). Each call to ref will add 1 to that.
func (v *Varchar) Ref(a uint64) (err error) {
	return v.doRun(a, func(m *varRunMeta) error {
		m.refs += 1
		return nil
	})
}

// Deref decremnents the ref field. If it ever reaches 0 it will
// automatically be freed (by calling `v.Free(a)`).
func (v *Varchar) Deref(a uint64) (err error) {
	doFree := false
	err = v.doRun(a, func(m *varRunMeta) error {
		m.refs -= 1
		if m.refs == 0 {
			doFree = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if doFree {
		return v.Free(a)
	}
	return nil
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

func (v *Varchar) doRun(a uint64, do func(*varRunMeta) error) error {
	return v.do(
		a,
		func(*varCtrl) error { return errors.Errorf("unexpected ctrl blk") },
		func(*varFree) error { return errors.Errorf("unexpected free blk") },
		do,
	)
}

func (v *Varchar) doFree(a uint64, do func(*varFree) error) error {
	return v.do(
		a,
		func(*varCtrl) error { return errors.Errorf("unexpected ctrl blk") },
		do,
		func(*varRunMeta) error { return errors.Errorf("unexpected run blk") },
	)
}

func (v *Varchar) doCtrl(do func(*varCtrl) error) error {
	return v.do(
		v.a,
		do,
		func(*varFree) error { return errors.Errorf("unexpected free blk") },
		func(*varRunMeta) error { return errors.Errorf("unexpected run blk") },
	)
}

func (v *Varchar) startOffsetBlks(a uint64) (offset, start, blks uint64) {
	blkSize := uint64(v.blkSize)
	offset = a % blkSize
	start = a - offset
	blks = 1
	if offset+varFreeSize > blkSize {
		blks = 2
	}
	return offset, start, blks
}

func (v *Varchar) do(
	a uint64,
	ctrlDo func(*varCtrl) error,
	freeDo func(*varFree) error,
	runDo func(*varRunMeta) error,
) error {
	offset, start, blks := v.startOffsetBlks(a)
	return v.bf.Do(start, blks, func(bytes []byte) error {
		bytes = bytes[offset:]
		flags := consts.AsFlag(bytes)
		if flags == consts.VARCHAR_CTRL {
			return ctrlDo(asCtrl(bytes))
		} else if flags == consts.VARCHAR_FREE {
			return freeDo(asFree(bytes))
		} else if flags == consts.VARCHAR_RUN {
			return runDo(asRunMeta(bytes))
		} else {
			return errors.Errorf("Unknown block type, %v", flags)
		}
	})
}

func (v *Varchar) unsafeGet(a uint64) ([]byte, error) {
	offset, start, blks := v.startOffsetBlks(a)
	bytes, err := v.bf.Get(start, blks)
	if err != nil {
		return nil, err
	}
	err = v.bf.Release(bytes)
	if err != nil {
		return nil, err
	}
	return bytes[offset:], nil
}

// This is for making new free segments. You probably (most definitely)
// want to use doFree.
func (v *Varchar) doAsFree(a uint64, do func(*varFree) error) error {
	offset, start, blks := v.startOffsetBlks(a)
	return v.bf.Do(start, blks, func(bytes []byte) error {
		bytes = bytes[offset:]
		return do(asFree(bytes))
	})
}

func (v *Varchar) newFree(a uint64, length int) error {
	if length < varFreeSize {
		return errors.Errorf("tried to free block which was too small to be a freeList node")
	}
	return v.doAsFree(a, func(m *varFree) error {
		m.flags = consts.VARCHAR_FREE
		m.length = uint32(length)
		m.list.prev = 0
		m.list.next = 0
		return nil
	})
}

// This is for making new run segments. You probably (most definitely)
// want to use doRun.
func (v *Varchar) doAsRun(a uint64, do func(*varRunMeta) error) error {
	offset, start, blks := v.startOffsetBlks(a)
	return v.bf.Do(start, blks, func(bytes []byte) error {
		bytes = bytes[offset:]
		return do(asRunMeta(bytes))
	})
}

// a == address of the start of the segment
// length == the requested length of the block
// fullLength == the length + meta data for the run. This is what is
//               actually allocated
// segLength == the length of the segment we are allocating in
func (v *Varchar) newRun(a uint64, length, fullLength, segLength int) (err error) {
	if fullLength > segLength {
		return errors.Errorf("tried to alloc in a segment smaller than the requested run")
	}
	err = v.doAsRun(a, func(m *varRunMeta) error {
		m.Init(length, fullLength-length-varRunMetaSize)
		return nil
	})
	if err != nil {
		return err
	}
	extra, err := v.freeExtra(a, segLength, fullLength)
	if err != nil {
		return err
	}
	return v.doRun(a, func(m *varRunMeta) error {
		if extra != 0 {
			m.extra += uint32(extra)
		}
		return nil
	})
}

func (v *Varchar) doFreeNode(a uint64, do func(*listNode) error) error {
	return v.doFree(a, func(m *varFree) error {
		return do(&m.list)
	})
}

func (v *Varchar) listInsert(node, prev, next uint64, doNode nodeDoer) error {
	if node == 0 {
		return errors.Errorf("0 offset for node (the inserted node)")
	}
	return doNode(node, func(n *listNode) error {
		if prev == 0 && next == 0 {
			n.prev = 0
			n.next = 0
			return nil
		} else if next == 0 {
			return doNode(prev, func(pn *listNode) error {
				n.next = 0
				n.prev = prev
				pn.next = node
				return nil
			})
		} else if prev == 0 {
			return doNode(next, func(nn *listNode) error {
				n.next = next
				n.prev = 0
				nn.prev = node
				return nil
			})
		}
		return doNode(prev, func(pn *listNode) error {
			return doNode(next, func(nn *listNode) error {
				n.next = next
				n.prev = prev
				pn.next = node
				nn.prev = node
				return nil
			})
		})
	})
}

func (v *Varchar) listRemove(node uint64, doNode nodeDoer) error {
	if node == 0 {
		return errors.Errorf("0 offset for node (the removed node)")
	}
	return doNode(node, func(n *listNode) (err error) {
		if n.prev != 0 {
			err = doNode(n.prev, func(pn *listNode) error {
				pn.next = n.next
				return nil
			})
			if err != nil {
				return nil
			}
		}
		if n.next != 0 {
			err = doNode(n.next, func(nn *listNode) error {
				nn.prev = n.prev
				return nil
			})
			if err != nil {
				return nil
			}
		}
		n.prev = 0
		n.next = 0
		return nil
	})
}
