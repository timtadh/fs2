package bptree

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/slice"
)

type leafMeta struct {
	baseMeta
	next    uint64
	prev    uint64
	valSize uint16
}

type leaf struct {
	meta  leafMeta
	bytes [consts.BLOCKSIZE - leafMetaSize]byte
}

const leafMetaSize = 32

var leafMetaSizeActual int

func init() {
	m := &leafMeta{}
	leafMetaSizeActual = int(m.Size())
	if leafMetaSizeActual != leafMetaSize {
		panic("the leafMeta was an unexpected size")
	}
}

func loadLeafMeta(backing []byte) *leafMeta {
	back := slice.AsSlice(&backing)
	return (*leafMeta)(back.Array)
}

func (m *leafMeta) Init(flags consts.Flag, keySize, keyCap, valSize uint16) {
	bm := &m.baseMeta
	bm.Init(flags, keySize, keyCap)
	m.next = 0
	m.prev = 0
	m.valSize = valSize
}

func (m *leafMeta) Size() uintptr {
	return reflect.TypeOf(*m).Size()
}

func (m *leafMeta) String() string {
	return fmt.Sprintf(
		"%v, valSize: %v, next: %v, prev: %v",
		&m.baseMeta, m.valSize, m.next, m.prev)
}

func (n *leaf) String() string {
	return fmt.Sprintf(
		"meta: <%v>, keys: <%v>, vals: <%v>",
		n.meta, n.keys()[:n.meta.keyCount*n.meta.keySize], n.vals()[:n.meta.keyCount*n.meta.valSize])
}

func (n *leaf) keyCount() int {
	return int(n.meta.keyCount)
}

func (n *leaf) _has(v *Varchar, key []byte) bool {
	_, has, err := find(v, n, key)
	if err != nil {
		log.Fatal(err)
	}
	return has
}

func (n *leaf) firstValue(vc *Varchar, key []byte) ([]byte, error) {
	i, has, err := find(vc, n, key)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errors.Errorf("leaf does not have that key")
	}
	v := n.val(i)
	if n.meta.flags&consts.VARCHAR_VALS != 0 {
		var value []byte
		err := vc.Do(*slice.AsUint64(&v), func(vbytes []byte) error {
			value = make([]byte, len(vbytes))
			copy(value, vbytes)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return value, nil
	} else {
		return v, nil
	}
}

func (n *leaf) doValueAt(vc *Varchar, i int, do func([]byte) error) error {
	flags := n.meta.flags
	if flags&consts.VARCHAR_VALS != 0 {
		return n.doBig(vc, n.val(i), do)
	} else {
		return do(n.val(i))
	}
}

func (n *leaf) doKeyAt(vc *Varchar, i int, do func([]byte) error) error {
	flags := n.meta.flags
	if flags&consts.VARCHAR_KEYS != 0 {
		return n.doBig(vc, n.key(i), do)
	} else {
		return do(n.key(i))
	}
}

func (n *leaf) unsafeKeyAt(vc *Varchar, i int) ([]byte, error) {
	flags := n.meta.flags
	if flags&consts.VARCHAR_KEYS != 0 {
		k := n.key(i)
		return vc.UnsafeGet(*slice.AsUint64(&k))
	} else {
		return n.key(i), nil
	}
}

func (n *leaf) doBig(vc *Varchar, v []byte, do func([]byte) error) error {
	return vc.Do(*slice.AsUint64(&v), func(bytes []byte) error {
		return do(bytes)
	})
}

func (n *leaf) key(i int) []byte {
	keySize := int(n.meta.keySize)
	s := keySize * i
	e := s + keySize
	return n.bytes[s:e]
}

func (n *leaf) val(i int) []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	valSize := int(n.meta.valSize)
	s := keyCap*keySize + valSize*i
	e := s + valSize
	return n.bytes[s:e]
}

func (n *leaf) keys() []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	s := 0
	e := s + keyCap*keySize
	return n.bytes[s:e]
}

func (n *leaf) vals() []byte {
	keySize := int(n.meta.keySize)
	keyCap := int(n.meta.keyCap)
	valSize := int(n.meta.valSize)
	s := keyCap * keySize
	e := s + keyCap*valSize
	return n.bytes[s:e]
}

func (n *leaf) fitsAnother() bool {
	return n.meta.keyCount+1 < n.meta.keyCap
}

// this method is totally UNSAFE
func (n *leaf) pure(v *Varchar) bool {
	if n.meta.keyCount == 0 {
		return true
	}
	var firstKey []byte
	err := n.doKeyAt(v, 0, func(k []byte) error {
		firstKey = k
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i < int(n.meta.keyCount); i++ {
		var key_i []byte
		err := n.doKeyAt(v, i, func(k []byte) error {
			key_i = k
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
		if !bytes.Equal(firstKey, key_i) {
			return false
		}
	}
	return true
}

func (n *leaf) putKV(v *Varchar, key []byte, value []byte) (err error) {
	if len(value) != int(n.meta.valSize) {
		return errors.Errorf("value was the wrong size")
	}
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.meta.keyCount+1 >= n.meta.keyCap {
		return errors.Errorf("block is full")
	}
	if !n.fitsAnother() {
		return errors.Errorf("block is full (value doesn't fit)")
	}

	var idx int
	if n.meta.flags&consts.VARCHAR_KEYS == 0 {
		idx, _, err = find(v, n, key)
	} else {
		err = v.Do(*slice.AsUint64(&key), func(key []byte) (err error) {
			idx, _, err = find(v, n, key)
			return err
		})
	}
	if err != nil {
		return err
	}

	keys := n.keys()
	vals := n.vals()
	keySize := int(n.meta.keySize)
	valSize := int(n.meta.valSize)
	if idx == int(n.meta.keyCount) {
		// fantastic we don't nee to move any thing.
		// we can just append
	} else {
		chunk := int(n.meta.keyCount) - idx
		// move the keys
		{
			chunkSize := chunk * keySize
			s := idx * keySize
			e := s + chunkSize
			from := keys[s:e]
			to := keys[s+keySize : e+keySize]
			copy(to, from)
		}
		// move the vals
		{
			chunkSize := chunk * valSize
			s := idx * valSize
			e := s + chunkSize
			from := vals[s:e]
			to := vals[s+valSize : e+valSize]
			copy(to, from)
		}
	}
	// do the book keeping
	n.meta.keyCount += 1
	// copy in the new key
	{
		s := idx * keySize
		e := s + keySize
		key_slice := keys[s:e]
		copy(key_slice, key)
	}
	// copy in the new val
	{
		s := idx * valSize
		e := s + valSize
		val_slice := vals[s:e]
		copy(val_slice, value)
	}

	idx = 0
	var has bool
	if n.meta.flags&consts.VARCHAR_KEYS == 0 {
		idx, has, err = find(v, n, key)
		if err != nil {
			return err
		}
	} else {
		err = v.Do(*slice.AsUint64(&key), func(key []byte) (err error) {
			idx, has, err = find(v, n, key)
			return err
		})
		if err != nil {
			return err
		}
	}
	if !has {
		return errors.Errorf("could not find key after put")
	}
	for ; idx < int(n.meta.keyCount); idx++ {
		if bytes.Equal(value, n.val(idx)) {
			return nil
		}
	}
	return errors.Errorf("could not find value after put")
}

func (n *leaf) delKV(v *Varchar, key []byte, which func([]byte) bool) error {
	if len(key) != int(n.meta.keySize) {
		return errors.Errorf("key was the wrong size")
	}
	if n.meta.keyCount <= 0 {
		return errors.Errorf("block is empty")
	}
	idx, has, err := find(v, n, key)
	if err != nil {
		return err
	}
	if !has {
		return errors.Errorf("that key was not in the block")
	}
	for ; idx < int(n.meta.keyCount); idx++ {
		var eq bool
		err = n.doKeyAt(v, idx, func(k []byte) error {
			eq = bytes.Equal(key, k)
			return nil
		})
		if err != nil {
			return err
		}
		if !eq {
			return errors.Errorf("no key removed")
		}
		var whichRes bool
		err = n.doValueAt(v, idx, func(v []byte) error {
			whichRes = which(v)
			return nil
		})
		if err != nil {
			return err
		}
		if whichRes {
			break
		}
	}
	return n.delItemAt(idx)
}

func (n *leaf) delItemAt(idx int) error {
	// ok we have our key_idx
	if idx+1 == int(n.meta.keyCount) {
		// sweet we can just drop the last
		// key value
		n.meta.keyCount--
		// zero out the old ones
		fmap.MemClr(n.key(idx))
		fmap.MemClr(n.val(idx))
		return nil
	}
	chunk := int(n.meta.keyCount) - idx - 1
	// drop the key
	{
		keys := n.keys()
		keySize := int(n.meta.keySize)
		chunkSize := chunk * keySize
		s := idx * keySize
		e := s + chunkSize
		from := keys[s+keySize : e+keySize]
		to := keys[s:e]
		copy(to, from)
	}
	// drop the value
	{
		vals := n.vals()
		valSize := int(n.meta.valSize)
		chunkSize := chunk * valSize
		s := idx * valSize
		e := s + chunkSize
		from := vals[s+valSize : e+valSize]
		to := vals[s:e]
		copy(to, from)
	}
	// do the book keeping
	n.meta.keyCount--
	return nil
}

func loadLeaf(backing []byte) (*leaf, error) {
	n := asLeaf(backing)
	if n.meta.flags&consts.LEAF == 0 {
		return nil, errors.Errorf("Was not a leaf node")
	}
	return n, nil
}

func asLeaf(backing []byte) *leaf {
	back := slice.AsSlice(&backing)
	return (*leaf)(back.Array)
}

func newLeaf(flags consts.Flag, backing []byte, keySize, valSize uint16) (*leaf, error) {
	n := asLeaf(backing)

	available := uintptr(len(backing)) - leafMetaSize

	kvSize := uintptr(keySize) + uintptr(valSize)
	keyCap := available / kvSize

	n.meta.Init(consts.LEAF|flags, keySize, uint16(keyCap), valSize)
	return n, nil
}
