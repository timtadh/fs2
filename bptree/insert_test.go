package bptree

import "testing"

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/slice"
)

func (t *T) bpt() (*BpTree, func()) {
	bf, bf_clean := t.blkfile()
	bpt, err := New(bf, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	return bpt, bf_clean
}

func (t *T) bptFixed() (*BpTree, func()) {
	bf, bf_clean := t.blkfile()
	bpt, err := New(bf, 8, 8)
	if err != nil {
		t.Fatal(err)
	}
	return bpt, bf_clean
}

type KV struct {
	key   []byte
	value []byte
}

type KVS []*KV

func (kvs KVS) Len() int {
	return len([]*KV(kvs))
}

func (kvs KVS) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}

func (kvs KVS) Less(i, j int) bool {
	return bytes.Compare(kvs[i].key, kvs[j].key) < 0
}

func (t *T) make_kv() *KV {
	return &KV{
		// key: t.rand_key(),
		key:   t.rand_varchar(8, 17),
		value: t.rand_varchar(1, 127),
	}
}

type KP struct {
	key []byte
	ptr uint64
}

type KPS []*KP

func (kps KPS) Len() int {
	return len([]*KP(kps))
}

func (kps KPS) Swap(i, j int) {
	kps[i], kps[j] = kps[j], kps[i]
}

func (kps KPS) Less(i, j int) bool {
	return bytes.Compare(kps[i].key, kps[j].key) < 0
}

func (t *T) make_kp() *KP {
	return &KP{
		key: t.rand_key(),
		ptr: t.key(t.rand_key()),
	}
}

func TestLeafInsert(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := ((consts.BLOCKSIZE - leafMetaSize) / 16) - 1
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, LEAF_CAP)
		// t.Log(n)
		for i := 0; i < LEAF_CAP; i++ {
			kv := t.make_kv()
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Add(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert(fmt.Sprintf("wrong key %v != %v", kv.key, k), bytes.Equal(kv.key, k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
				return nil
			}))
		}
		clean()
	}
}

/*
DISABLED AFTER EVISCERATION
func TestLeafBigInsert(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 152
	for TEST := 0; TEST < 10; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, LEAF_CAP)
		for i := 0; i < LEAF_CAP; i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_bigValue(2048, 4096*5),
			}
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Add(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.bf, kv.key))
				return nil
			}))
		}
		for _, kv := range kvs {
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.bf, kv.key))
				return nil
			}))
		}
		clean()
	}
}
*/

func TestLeafSplit(x *testing.T) {
	t := (*T)(x)
	CAP := 300
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, CAP)
		for i := 0; i < CAP; i++ {
			kv := t.make_kv()
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Add(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			if !bytes.Equal(kv.key, k) {
				t.Log(a, i)
			}
			t.assert(fmt.Sprintf("wrong key %v != %v", kv.key, k), bytes.Equal(kv.key, k))
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
				return nil
			}))
		}
		clean()
	}
}

func TestInsert3Level(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	kvs := make([]*KV, 0, 100000)
	// t.Log(n)
	for i := 0; i < cap(kvs); i++ {
		var kv *KV
		kv = t.make_kv()
		kvs = append(kvs, kv)
		// t.Log(i, len(kvs))
	}
	// t.Log("starting insert")
	for _, kv := range kvs {
		// t.Log(i, len(kvs))
		t.assert_nil(bpt.Add(kv.key, kv.value))
		t.assert_has(bpt)(kv.key)
	}
	t.assert_nil(bpt.Verify())
	// t.Log("start existence check")
	for _, kv := range kvs {
		// t.Log(i, len(kvs))
		a, i, err := bpt.getStart(kv.key)
		t.assert_nil(err)
		k, err := bpt.keyAt(a, i)
		t.assert_nil(err)
		t.assert(fmt.Sprintf("wrong key %v == %v", kv.key, k), bytes.Equal(kv.key, k))
		t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
			t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
			return nil
		}))
	}
	clean()
}

func TestEndOfPureRun(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < 1; TEST++ {
		bpt, clean := t.bptFixed()
		kvs := make([]*KV, 0, 2000)
		start, err := bpt.newLeaf()
		t.assert_nil(err)
		cur := start
		for i := 0; i < rand.Intn(500)+255; i++ {
			kv := &KV{
				key:   []byte{1, 0, 1, 0, 1, 0, 1, 0},
				value: t.rand_value(8),
			}
			kvs = append(kvs, kv)
			var fits bool = false
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				fits = cur.fitsAnother()
				return nil
			}))
			if !fits {
				next, err := bpt.newLeaf()
				t.assert_nil(err)
				t.assert_nil(bpt.insertListNode(next, cur, 0))
				cur = next
			}
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				return cur.putKV(bpt.varchar, kv.key, kv.value)
			}))
		}
		end, err := bpt.endOfPureRun(start)
		t.assert_nil(err)
		t.assert("end should be cur", end == cur)
		clean()
	}
}

func TestEndOfPureRunVarchar(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < 1; TEST++ {
		bpt, clean := t.bpt()
		k, err := bpt.varchar.Alloc(17)
		t.assert_nil(err)
		t.assert_nil(bpt.varchar.Do(k, func(data []byte) error {
			copy(data, []byte{1, 0, 1, 0, 1, 0, 1, 0})
			return nil
		}))
		KEY := slice.Uint64AsSlice(&k)
		kvs := make([]*KV, 0, 2000)
		start, err := bpt.newLeaf()
		t.assert_nil(err)
		cur := start
		for i := 0; i < rand.Intn(500)+255; i++ {
			kv := &KV{
				key:   KEY,
				value: t.rand_value(8),
			}
			kvs = append(kvs, kv)
			var fits bool = false
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				fits = cur.fitsAnother()
				return nil
			}))
			if !fits {
				next, err := bpt.newLeaf()
				t.assert_nil(err)
				t.assert_nil(bpt.insertListNode(next, cur, 0))
				cur = next
			}
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				return cur.putKV(bpt.varchar, kv.key, kv.value)
			}))
		}
		end, err := bpt.endOfPureRun(start)
		t.assert_nil(err)
		t.assert("end should be cur", end == cur)
		clean()
	}
}

func TestInternalSplit(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS*10; TEST++ {
		bpt, clean := t.bptFixed()
		kps := make(KPS, 0, 254)
		a, err := bpt.newInternal()
		t.assert_nil(err)
		for i := 0; i < cap(kps); i++ {
			kp := t.make_kp()
			kps = append(kps, kp)
			t.assert_nil(bpt.doInternal(a, func(a *internal) error {
				return a.putKP(bpt.varchar, kp.key, kp.ptr)
			}))
		}
		sort.Sort(kps)
		split_kp := t.make_kp()
		p, q, err := bpt.internalSplit(a, split_kp.key, split_kp.ptr)
		t.assert_nil(err)
		t.assert("p should equal a", a == p)
		t.assert(fmt.Sprintf("q, %v, should equal %v", q, p+uint64(bpt.bf.BlockSize())), q != 0)
		i := 0
		var found_split bool = false
		t.assert_nil(bpt.doInternal(p, func(p *internal) error {
			for ; i < len(kps); i++ {
				kp := kps[i]
				j, has, err := find(bpt.varchar, p, kp.key)
				t.assert_nil(err)
				if !has {
					break
				}
				t.assert("keys should equal", t.key(p.key(j)) == t.key(kp.key))
				t.assert("ptrs should equal", *p.ptr(j) == kp.ptr)
			}
			j, has, err := find(bpt.varchar, p, split_kp.key)
			t.assert_nil(err)
			if !has {
				return nil
			}
			t.assert("split keys should equal", t.key(p.key(j)) == t.key(split_kp.key))
			t.assert("split ptrs should equal", *p.ptr(j) == split_kp.ptr)
			found_split = true
			return nil
		}))
		t.assert_nil(bpt.doInternal(q, func(q *internal) error {
			for ; i < len(kps); i++ {
				kp := kps[i]
				j, has, err := find(bpt.varchar, q, kp.key)
				t.assert_nil(err)
				if !has {
					break
				}
				t.assert("keys should equal", t.key(q.key(j)) == t.key(kp.key))
				t.assert("ptrs should equal", *q.ptr(j) == kp.ptr)
			}
			j, has, err := find(bpt.varchar, q, split_kp.key)
			t.assert_nil(err)
			if !has {
				return nil
			}
			t.assert("split keys should equal", t.key(q.key(j)) == t.key(split_kp.key))
			t.assert("split ptrs should equal", *q.ptr(j) == split_kp.ptr)
			found_split = true
			return nil
		}))
		t.assert(fmt.Sprintf("i, %v, == len(kps) %v", i, len(kps)), i == len(kps))
		t.assert("should find split", found_split)
		clean()
	}
}

func TestInternalInsertSplit(x *testing.T) {
	t := (*T)(x)
	LEAF_CAP := 253
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bptFixed()
		kvs := make(KVS, 0, LEAF_CAP*2)
		for i := 0; i < cap(kvs); i++ {
			kv := &KV{
				key:   t.rand_key(),
				value: t.rand_key(),
			}
			kvs = append(kvs, kv)
		}
		sort.Sort(kvs)
		a, err := bpt.newLeaf()
		t.assert_nil(err)
		b, err := bpt.newLeaf()
		t.assert_nil(err)
		I, err := bpt.newInternal()
		t.assert_nil(err)
		t.assert_nil(bpt.doInternal(I, func(I *internal) error {
			I.meta.keyCap = 3
			return nil
		}))
		bpt.meta.root = I
		t.assert_nil(bpt.writeMeta())
		t.assert_nil(bpt.doInternal(I, func(I *internal) error {
			v0, err := bpt.checkValue(kvs[0].value)
			t.assert_nil(err)
			t.assert_nil(bpt.doLeaf(a, func(a *leaf) error {
				return a.putKV(bpt.varchar, kvs[0].key, v0)
			}))
			vL, err := bpt.checkValue(kvs[LEAF_CAP].value)
			t.assert_nil(err)
			t.assert_nil(bpt.doLeaf(b, func(b *leaf) error {
				return b.putKV(bpt.varchar, kvs[LEAF_CAP].key, vL)
			}))
			t.assert_nil(I.putKP(bpt.varchar, kvs[0].key, a))
			t.assert_nil(I.putKP(bpt.varchar, kvs[LEAF_CAP].key, b))
			return nil
		}))
		for i := 1; i < LEAF_CAP; i++ {
			kv := kvs[i]
			v, err := bpt.checkValue(kv.value)
			t.assert_nil(err)
			p, q, err := bpt.leafInsert(a, kv.key, v, true)
			t.assert_nil(err)
			t.assert(fmt.Sprintf("p should be a, at idx %v, cap %v", i, LEAF_CAP), p == a)
			t.assert(fmt.Sprintf("q should be 0 a, at idx %v, cap %v", i, LEAF_CAP), q == 0)
		}
		for i := LEAF_CAP + 1; i < len(kvs); i++ {
			kv := kvs[i]
			v, err := bpt.checkValue(kv.value)
			t.assert_nil(err)
			p, q, err := bpt.leafInsert(b, kv.key, v, true)
			t.assert_nil(err)
			t.assert(fmt.Sprintf("p should be b, at idx %v, cap %v", i, LEAF_CAP), p == b)
			t.assert(fmt.Sprintf("q should be 0 a, at idx %v, cap %v", i, LEAF_CAP), q == 0)
		}
		split_kv := &KV{
			key:   t.rand_key(),
			value: t.rand_key(),
		}
		sv, err := bpt.checkValue(split_kv.value)
		t.assert_nil(err)
		p, q, err := bpt.internalInsert(I, split_kv.key, sv, true)
		t.assert_nil(err)
		t.assert("p should be I", p == I)
		t.assert("q should not be 0", q != 0)
		root, err := bpt.newInternal()
		t.assert_nil(err)
		t.assert_nil(bpt.doInternal(root, func(n *internal) error {
			t.assert_nil(bpt.firstKey(p, func(pkey []byte) error {
				return n.putKP(bpt.varchar, pkey, p)
			}))
			return bpt.firstKey(q, func(qkey []byte) error {
				return n.putKP(bpt.varchar, qkey, q)
			})
		}))
		bpt.meta.root = root
		t.assert_nil(bpt.writeMeta())
		for _, kv := range kvs {
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			k1 := t.key(kv.key)
			k2 := t.key(k)
			t.assert(fmt.Sprintf("wrong key %v == %v", k1, k2), k1 == k2)
			t.assert_nil(bpt.doLeaf(a, func(n *leaf) error {
				t.assert_value(kv.value)(n.firstValue(bpt.varchar, kv.key))
				return nil
			}))
		}
		{
			a, i, err := bpt.getStart(split_kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			k1 := t.key(split_kv.key)
			k2 := t.key(k)
			t.assert(fmt.Sprintf("wrong key %v == %v", k1, k2), k1 == k2)
		}
		clean()
	}
}
