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

type listNode struct {
	prev uint64
	next uint64
}

type nodeDoer func(uint64, func(*listNode) error) error

type varFree struct {
	flags consts.Flag
	length uint32
	list listNode
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

func (vrm *varRunMeta) Init(length, extra int) {
	vrm.flags = consts.VARCHAR_RUN
	vrm.length = uint32(length)
	vrm.extra = uint32(extra)
	vrm.refs = 1
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

// Allocate a varchar of the desired length.
func (v *Varchar) Alloc(length int) (a uint64, err error) {
	if uint32(length) >= maxArraySize {
		return 0, errors.Errorf("Size is too large. Cannon allocate an array that big")
	}
	newAlloc := false
	fullLength := v.allocAmt(length)
	err = v.doCtrl(func(ctrl *varCtrl) error {
		if ctrl.freeLen == 0 {
			newAlloc = true
			return nil
		}
		found := false
		cur := ctrl.freeHead
		for i := 0; i < int(ctrl.freeLen); i++ {
			err = v.doFree(cur, func(n *varFree) error {
				if fullLength <= int(n.length) {
					found = true
					a = cur
					if cur == ctrl.freeHead {
						ctrl.freeHead = n.list.next
					}
					ctrl.freeLen--
					err = v.listRemove(cur, v.doFreeNode)
					if err != nil {
						return err
					}
					return v.newRun(cur, length, fullLength, int(n.length))
				}
				cur = n.list.next
				return nil
			})
			if err != nil {
				return err
			}
			if found {
				break
			}
		}
		if !found {
			newAlloc = true
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if newAlloc {
		return v.allocNew(length, fullLength)
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
	err = v.newRun(a, length, fullLength, blks * int(v.bf.BlockSize()))
	if err != nil {
		return 0, err
	}
	return a, nil
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
		length = int(m.length + m.extra) + varRunMetaSize
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
	return v.doCtrl(func(ctrl *varCtrl) error {
		var prev, cur uint64
		cur = ctrl.freeHead
		listLen := int(ctrl.freeLen)
		found := false
		for i := 0; i < listLen; i++ {
			err = v.doFree(cur, func(p *varFree) error {
				if a < cur {
					found = true
					return nil
				}
				prev = cur
				cur = p.list.next
				return nil
			})
			if err != nil {
				return err
			}
			if found {
				break
			}
		}
		ctrl.freeLen++
		err = v.listInsert(a, prev, cur, v.doFreeNode)
		if err != nil {
			return err
		}
		if prev == 0 {
			ctrl.freeHead = a
		}
		return v.coallesce(ctrl, a)
	})
}

// coallesce blocks centered around "a". The address "a" may not be a
// block after this function returns.
func (v *Varchar) coallesce(ctrl *varCtrl, a uint64) (err error) {
	return v.doFree(a, func(c *varFree) error {
		if c.list.next != 0 && a + uint64(c.length) == c.list.next {
			err = v.joinNext(ctrl, a)
			if err != nil {
				return err
			}
		}
		if c.list.prev != 0 {
			return v.doFree(c.list.prev, func(p *varFree) error {
				if c.list.prev + uint64(p.length) == a {
					return v.joinNext(ctrl, c.list.prev)
					// the block at the previous "a" no longer exists.
					// freeHead should not need to be adjusted because
					// 1. the old "a" could not have been the head 
					//    since prev != 0
					// 2. if c.list.prev was the head, joinNext will not
					//    change that
				}
				return nil
			})
		}
		return nil
	})
}


// joins the block at "a" to the next block. The next block is removed
// from the free list and freeLen is decremented. Note, this will have
// no effect on the freeHead.
func (v *Varchar) joinNext(ctrl *varCtrl, a uint64) (err error) {
	return v.doFree(a, func(c *varFree) error {
		next := c.list.next
		if next == 0 || a + uint64(c.length) != next {
			return errors.Errorf("invalid joinNext call")
		}
		ctrl.freeLen--
		err = v.listRemove(next, v.doFreeNode)
		if err != nil {
			return err
		}
		return v.doFree(next, func(n *varFree) error {
			c.length += n.length
			n.flags = 0xff
			n.length = 0
			return nil
		})
	})
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
		for offset + uint64(fullLength) >= blks * uint64(v.bf.BlockSize()) {
			blks++
		}
		size, err := v.bf.Size()
		if err != nil {
			return err
		}
		for start + blks * uint64(v.bf.BlockSize()) > uint64(size) {
			blks--
		}
		return v.bf.Do(start, blks, func(bytes []byte) error {
			bytes = bytes[offset:]
			flags := consts.Flag(bytes[0])
			if flags & consts.VARCHAR_RUN == 0 {
				return errors.Errorf("bad address, was not a run block")
			}
			r := asRun(bytes)
			return do(r.bytes[:r.meta.length])
		})
	})
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
	blkSize := uint64(v.bf.BlockSize())
	offset = a % blkSize
	start = a - offset
	blks = 1
	if offset + varFreeSize > blkSize {
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
		flags := consts.Flag(bytes[0])
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
		m.Init(length, fullLength - length - varRunMetaSize)
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

