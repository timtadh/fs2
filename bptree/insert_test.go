package bptree

import "testing"

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
)

func (t *T) bpt() (*BpTree, func()) {
	bf, bf_clean := t.blkfile()
	bpt, err := New(bf, 8)
	if err != nil {
		t.Fatal(err)
	}
	return bpt, bf_clean
}

type KV struct {
	key []byte
	value []byte
}

func (t *T) make_kv() *KV {
	return &KV{
		key: t.rand_key(),
		value: t.rand_value(24),
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
	for TEST := 0; TEST < TESTS; TEST++ {
		SIZE := 1027+TEST*16
		bpt, clean := t.bpt()
		n, err := newLeaf(make([]byte, SIZE), 8)
		t.assert_nil(err)
		kvs := make([]*KV, 0, n.meta.keyCap/2)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := t.make_kv()
			if !n.fits(kv.value) {
				break
			}
			kvs = append(kvs, kv)
			t.assert_nil(n.putKV(kv.key, kv.value))
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
		}
		clean()
	}
}

func TestLeafSplit(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, 200)
		// t.Log(n)
		for i := 0; i < cap(kvs); i++ {
			kv := t.make_kv()
			kvs = append(kvs, kv)
			t.assert_nil(bpt.Put(kv.key, kv.value))
			a, i, err := bpt.getStart(kv.key)
			t.assert_nil(err)
			k, err := bpt.keyAt(a, i)
			t.assert_nil(err)
			t.assert("wrong key", t.key(kv.key) == t.key(k))
		}
		clean()
	}
}

func TestInsert3Level(x *testing.T) {
	t := (*T)(x)
	bpt, clean := t.bpt()
	kvs := make([]*KV, 0, 200000)
	// t.Log(n)
	for i := 0; i < cap(kvs); i++ {
		kv := t.make_kv()
		kvs = append(kvs, kv)
		t.assert_nil(bpt.Put(kv.key, kv.value))
		a, i, err := bpt.getStart(kv.key)
		t.assert_nil(err)
		k, err := bpt.keyAt(a, i)
		t.assert_nil(err)
		k1 := t.key(kv.key)
		k2 := t.key(k)
		t.assert(fmt.Sprintf("wrong key %v == %v", k1, k2), k1 == k2)
	}
	clean()
}

func TestEndOfPureRun(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS; TEST++ {
		bpt, clean := t.bpt()
		kvs := make([]*KV, 0, 1000)
		start, err := bpt.newLeaf()
		t.assert_nil(err)
		cur := start
		for i := 0; i < rand.Intn(500) + 250; i++ {
			kv := &KV{
				key: []byte{1,0,1,0,1,0,1,0},
				value: t.rand_value(24),
			}
			kvs = append(kvs, kv)
			var fits bool = false
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				fits = cur.fits(kv.value)
				return nil
			}))
			if fits {
				next, err := bpt.newLeaf()
				t.assert_nil(err)
				t.assert_nil(insertListNode(bpt.bf, next, cur, 0))
				cur = next
			}
			t.assert_nil(bpt.doLeaf(cur, func(cur *leaf) error {
				return cur.putKV(kv.key, kv.value)
			}))
		}
		end, err := bpt.endOfPureRun(cur)
		t.assert_nil(err)
		t.assert("end should be cur", end == cur)
		clean()
	}
}

func TestInternalSplit(x *testing.T) {
	t := (*T)(x)
	for TEST := 0; TEST < TESTS*10; TEST++ {
		bpt, clean := t.bpt()
		kps := make(KPS, 0, 254)
		a, err := bpt.newInternal()
		t.assert_nil(err)
		for i := 0; i < cap(kps); i++ {
			kp := t.make_kp()
			kps = append(kps, kp)
			t.assert_nil(bpt.doInternal(a, func(a *internal) error {
				return a.putKP(kp.key, kp.ptr)
			}))
		}
		sort.Sort(kps)
		split_kp := t.make_kp()
		p, q, err := bpt.internalSplit(a, split_kp.key, split_kp.ptr)
		t.assert_nil(err)
		t.assert("p should equal a", a == p)
		t.assert(fmt.Sprintf("q, %v, should equal %v", q, p + uint64(bpt.bf.BlockSize())), q == a + uint64(bpt.bf.BlockSize()))
		i := 0
		var found_split bool = false
		t.assert_nil(bpt.doInternal(p, func(p *internal) error {
			for ; i < len(kps); i++ {
				kp := kps[i]
				j, has := find(int(p.meta.keyCount), p.keys, kp.key)
				if !has {
					break
				}
				t.assert("keys should equal", t.key(p.keys[j]) == t.key(kp.key))
				t.assert("ptrs should equal", p.ptrs[j] == kp.ptr)
			}
			j, has := find(int(p.meta.keyCount), p.keys, split_kp.key)
			if !has {
				return nil
			}
			t.assert("split keys should equal", t.key(p.keys[j]) == t.key(split_kp.key))
			t.assert("split ptrs should equal", p.ptrs[j] == split_kp.ptr)
			found_split = true
			return nil
		}))
		t.assert_nil(bpt.doInternal(q, func(q *internal) error {
			for ; i < len(kps); i++ {
				kp := kps[i]
				j, has := find(int(q.meta.keyCount), q.keys, kp.key)
				if !has {
					break
				}
				t.assert("keys should equal", t.key(q.keys[j]) == t.key(kp.key))
				t.assert("ptrs should equal", q.ptrs[j] == kp.ptr)
			}
			j, has := find(int(q.meta.keyCount), q.keys, split_kp.key)
			if !has {
				return nil
			}
			t.assert("split keys should equal", t.key(q.keys[j]) == t.key(split_kp.key))
			t.assert("split ptrs should equal", q.ptrs[j] == split_kp.ptr)
			found_split = true
			return nil
		}))
		t.assert(fmt.Sprintf("i, %v, == len(kps) %v", i, len(kps)), i == len(kps))
		t.assert("should find split", found_split)
		clean()
	}
}

