package bptree

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type baseMeta struct {
	flags    consts.Flag
	keySize  uint16
	keyCount uint16
	keyCap   uint16
}

const ptrSize = 8

const baseMetaSize = 8

var baseMetaSizeActual int

func init() {
	m := &baseMeta{}
	baseMetaSizeActual = int(m.Size())
	if baseMetaSizeActual != baseMetaSize {
		panic("the baseMeta was an unexpected size")
	}
}

type internal struct {
	meta  baseMeta
	bytes [consts.BLOCKSIZE - baseMetaSize]byte
}

func loadBaseMeta(backing []byte) *baseMeta {
	back := slice.AsSlice(&backing)
	return (*baseMeta)(back.Array)
}

func (m *baseMeta) Init(flags consts.Flag, keySize, keyCap uint16) {
	m.flags = flags
	m.keySize = keySize
	m.keyCount = 0
	m.keyCap = keyCap
}

func (m *baseMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *baseMeta) String() string {
	return fmt.Sprintf(
		"flags: %v, keySize: %v, keyCount: %v, keyCap: %v",
		m.flags, m.keySize, m.keyCount, m.keyCap)
}

func (n *internal) String() string {
	return fmt.Sprintf(
		"<internal meta: <%v>, keys: <%v>, ptrs: <%v>>",
		n.meta, n._keys(), n.ptrs_uint64s())
}

func (n *internal) Debug(v *Varchar) string {
	return fmt.Sprintf(
		"<internal (debug) (vchar %v) meta: <%v>, keys: <%v>, ptrs: <%v>>",
		v, n.meta, n._realKeys(v), n.ptrs_uint64s())
}

func (n *internal) key(i int) []byte {
	keySize := int(n.meta.keySize)
	s := i * keySize
	e := s + keySize
	return n.bytes[s:e]
}

// this is for debugging
func (n *internal) _keys() [][]byte {
	keys := make([][]byte, 0, n.meta.keyCount)
	for i := 0; i < int(n.meta.keyCount); i++ {
		keys = append(keys, n.key(i))
	}
	return keys
}

// this is for debugging
func (n *internal) _realKeys(v *Varchar) [][]byte {
	keys := make([][]byte, 0, n.meta.keyCount)
	for i := 0; i < int(n.meta.keyCount); i++ {
		err := n.doKeyAt(v, i, func(key []byte) error {
			k := make([]byte, len(key))
			copy(k, key)
			keys = append(keys, k)
			return nil
		})
		if err != nil {
			log.Println(i, err)
			keys = append(keys, []byte{0, 0, 1, 1, 0, 0, 1, 1, 0, 0})
		}
	}
	return keys
}

func (n *internal) ptr(i int) *uint64 {
	ptr := uintptr(unsafe.Pointer(n))
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := baseMetaSize + keyCap*keySize + i*ptrSize
	p := ptr + uintptr(s)
	return (*uint64)(unsafe.Pointer(p))
}

func (n *internal) ptrs() []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := keyCap * keySize
	e := s + keyCap*ptrSize
	return n.bytes[s:e]
}

func (n *internal) ptrs_uint64s() []uint64 {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := keyCap * keySize
	e := s + keyCap*ptrSize
	bytes := n.bytes[s:e]
	sl := slice.AsSlice(&bytes)
	sl.Len = int(n.meta.keyCount)
	sl.Cap = sl.Len
	return *sl.AsUint64s()
}

func (n *internal) keyCount() int {
	return int(n.meta.keyCount)
}

func (n *internal) cmpKeyAt(vc *Varchar, i int, key []byte) (cmp int, err error) {
	err = n.doKeyAt(vc, i, func(key_i []byte) error {
		cmp = bytes.Compare(key, key_i)
		return nil
	})
	return cmp, err
}

func (n *internal) doKeyAt(vc *Varchar, i int, do func([]byte) error) error {
	flags := n.meta.flags
	if flags&consts.VARCHAR_KEYS != 0 {
		return n.doBig(vc, n.key(i), do)
	} else {
		return do(n.key(i))
	}
}

func (n *internal) unsafeKeyAt(vc *Varchar, i int) ([]byte, error) {
	flags := n.meta.flags
	if flags&consts.VARCHAR_KEYS != 0 {
		k := n.key(i)
		return vc.UnsafeGet(*slice.AsUint64(&k))
	} else {
		return n.key(i), nil
	}
}

func (n *internal) doBig(vc *Varchar, v []byte, do func([]byte) error) error {
	return vc.Do(*slice.AsUint64(&v), func(bytes []byte) error {
		return do(bytes)
	})
}

func (n *internal) full() bool {
	return n.meta.keyCount+1 >= n.meta.keyCap
}

func (n *internal) findPtr(v *Varchar, key []byte) (uint64, error) {
	i, has, err := find(v, n, key)
	if err != nil {
		return 0, err
	}
	if !has {
		return 0, errors.Errorf("key was not in the internal node")
	}
	return *n.ptr(i), nil
}

func (n *internal) _has(v *Varchar, key []byte) bool {
	_, has, err := find(v, n, key)
	if err != nil {
		log.Fatal(err)
	}
	return has
}

func (n *internal) updateK(v *Varchar, i int, key []byte) error {
	if i < 0 || i >= int(n.meta.keyCount) {
		return errors.Errorf("key is out of range")
	}
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	idx, has, err := find(v, n, key)
	if err != nil {
		return err
	}
	if has && i != idx {
		log.Println(n.Debug(v))
		n.doKeyAt(v, idx, func(x []byte) error {
			k := n.key(idx)
			if v != nil {
				y, e := v.UnsafeGet(*slice.AsUint64(&k))
				log.Println(e)
				log.Println("key", x, y)
			} else {
				log.Println("key", x, "v was nil")
			}
			return nil
		})
		n.doKeyAt(v, i, func(x []byte) error {
			log.Println("replacing", x)
			return nil
		})
		return errors.Errorf("internal already had key %v, at %v, was going to put it at %v, replacing %v", key, idx, i, n.key(i))
	}
	oldk := make([]byte, len(n.key(i)))
	copy(oldk, n.key(i))
	flags := n.meta.flags
	if flags&consts.VARCHAR_KEYS != 0 {
		err := n.bigUpdateK(v, i, key)
		if err != nil {
			return err
		}
	} else {
		copy(n.key(i), key)
	}
	/*
		err = checkOrder(v, n)
		if err != nil {
			log.Println("replaced key", oldk)
			log.Println(n.Debug(v))
			return err
		}
	*/
	return nil
}

func (n *internal) bigUpdateK(v *Varchar, i int, key []byte) (err error) {
	old_key := n.key(i)
	err = v.Deref(*slice.AsUint64(&old_key))
	if err != nil {
		return err
	}
	err = v.Ref(*slice.AsUint64(&key))
	if err != nil {
		return err
	}
	copy(old_key, key) // old_key is a pointer into the block
	return nil
}

func (n *internal) putKP(v *Varchar, key []byte, p uint64) (err error) {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.full() {
		return errors.Errorf("block is full")
	}
	if n.meta.flags&consts.VARCHAR_KEYS != 0 {
		err = v.Ref(*slice.AsUint64(&key))
		if err != nil {
			return err
		}
	}
	err = n.putKey(v, key, func(i int) error {
		ptrs := n.ptrs()
		chunkSize := (int(n.meta.keyCount) - i) * ptrSize
		s := i * ptrSize
		from := ptrs[s : s+chunkSize]
		to := ptrs[s+ptrSize : s+chunkSize+ptrSize]
		copy(to, from)
		*n.ptr(i) = p
		return nil
	})
	if err != nil {
		return err
	}
	n.meta.keyCount++
	/*
		err = checkOrder(v, n)
		if err != nil {
			log.Println(n.Debug(v))
			return err
		}
	*/
	return nil
}

func (n *internal) delKP(v *Varchar, key []byte) error {
	i, has, err := find(v, n, key)
	if err != nil {
		return err
	}
	if !has {
		return errors.Errorf("key was not in the internal node")
	} else if i < 0 {
		return errors.Errorf("find returned a negative int")
	} else if i >= int(n.meta.keyCount) {
		return errors.Errorf("find returned a int > than len(keys)")
	}
	return n.delItemAt(v, i)
}

func (n *internal) delItemAt(v *Varchar, i int) error {
	// remove the key
	err := n.delKeyAt(v, i)
	if err != nil {
		return err
	}
	// remove the ptr
	ptrs := n.ptrs()
	chunkSize := (int(n.meta.keyCount) - i - 1) * ptrSize
	s := i * ptrSize
	from := ptrs[s+ptrSize : s+ptrSize+chunkSize]
	to := ptrs[s : s+chunkSize]
	copy(to, from)
	*n.ptr(int(n.meta.keyCount - 1)) = 0
	// do the book keeping
	n.meta.keyCount--
	/*
		err = checkOrder(v, n)
		if err != nil {
			log.Println("del at", i)
			log.Println(n.Debug(v))
			return err
		}
	*/
	return nil
}

func (n *internal) putKey(v *Varchar, key []byte, put func(i int) error) (err error) {
	if n.keyCount()+1 >= int(n.meta.keyCap) {
		return errors.Errorf("Block is full.")
	}

	var i int
	var has bool
	if n.meta.flags&consts.VARCHAR_KEYS == 0 {
		i, has, err = find(v, n, key)
	} else {
		err = v.Do(*slice.AsUint64(&key), func(key []byte) (err error) {
			i, has, err = find(v, n, key)
			return err
		})
	}
	if err != nil {
		return err
	}

	if i < 0 {
		return errors.Errorf("find returned a negative int")
	} else if i >= int(n.meta.keyCap) {
		return errors.Errorf("find returned a int > than len(keys)")
	} else if has {
		return errors.Errorf(fmt.Sprintf("would have inserted a duplicate key, %v", key))
	}
	if err := n.putKeyAt(key, i); err != nil {
		return err
	}
	return put(i)
}

func (n *internal) putKeyAt(key []byte, i int) error {
	if i < 0 || i > int(n.meta.keyCount) {
		return errors.Errorf("i was not in range")
	}
	for j := int(n.meta.keyCount) + 1; j > i; j-- {
		copy(n.key(j), n.key(j-1))
	}
	copy(n.key(i), key)
	return nil
}

func (n *internal) delKeyAt(v *Varchar, i int) (err error) {
	if n.meta.keyCount == 0 {
		return errors.Errorf("The items slice is empty")
	}
	if i < 0 || i >= int(n.meta.keyCount) {
		return errors.Errorf("i was not in range")
	}
	if n.meta.flags&consts.VARCHAR_KEYS != 0 {
		k := n.key(i)
		err = v.Deref(*slice.AsUint64(&k))
		if err != nil {
			return err
		}
	}
	for j := i; j+1 < int(n.meta.keyCount); j++ {
		copy(n.key(j), n.key(j+1))
	}
	// zero the old
	fmap.MemClr(n.key(int(n.meta.keyCount - 1)))
	return nil
}

func loadInternal(backing []byte) (*internal, error) {
	n := asInternal(backing)
	if n.meta.flags&consts.INTERNAL == 0 {
		return nil, errors.Errorf("Was not an internal node")
	}
	return n, nil
}

func keysPerInternal(blockSize int, keySize int) int {
	available := blockSize - int((&baseMeta{}).Size())
	ptrSize := 8
	kvSize := keySize + ptrSize
	keyCap := available / kvSize
	return keyCap
}

func asInternal(backing []byte) *internal {
	back := slice.AsSlice(&backing)
	return (*internal)(back.Array)
}

func newInternal(flags consts.Flag, backing []byte, keySize uint16) (*internal, error) {
	n := asInternal(backing)

	keyCap := uint16(keysPerInternal(len(backing), int(keySize)))
	n.meta.Init(consts.INTERNAL|flags, keySize, keyCap)

	return n, nil
}
