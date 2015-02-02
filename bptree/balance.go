package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/fmap"
)

func (a *internal) balance(b *internal) error {
	if b.meta.keyCount != 0 {
		return Errorf("b was not empty")
	}
	var m int = a.balancePoint()
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		copy(b.keys[i], a.keys[j])
		fmap.MemClr(a.keys[j])
		b.ptrs[i] = a.ptrs[j]
		a.ptrs[j] = 0
	}
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	return nil
}

func (a *leaf) balance(b *leaf) error {
	if b.meta.keyCount != 0 {
		return Errorf("b was not empty")
	}
	var m int = a.balancePoint()
	if m == 0 {
		// we had a pure balance
		return b.reattachLeaf()
	}
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		b.valueSizes[i] = a.valueSizes[j]
		a.valueSizes[j] = 0
	}
	m_offset := a.keyOffset(m)
	from := a.kvs[m_offset:]
	copy(b.kvs, from)
	fmap.MemClr(from)
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	if err := a.reattachLeaf(); err != nil {
		return err
	}
	if err := b.reattachLeaf(); err != nil {
		return err
	}
	return nil
}

func (n *internal) balancePoint() int {
	m := int(n.meta.keyCount)/2
	return noSplitBalancePoint(n.keys, int(n.meta.keyCount), m)
}

func (n *leaf) balancePoint() int {
	if n.meta.keyCount == 0 {
		return 0
	}
	length := n.next_kv_in_kvs()
	guess := length / 2
	m := 0
	for m + 1 < int(n.meta.keyCount) {
		if n.keyOffset(m+1) > guess {
			break
		}
		m++
	}
	return noSplitBalancePoint(n.keys, int(n.meta.keyCount), m)
}

func noSplitBalancePoint(keys [][]byte, keyCount, m int) int {
	for m < keyCount && bytes.Equal(keys[m-1], keys[m]) {
		m++
	}
	if m >= keyCount && m > 0 {
		m--
		for m > 0 && bytes.Equal(keys[m-1], keys[m]) {
			m--
		}
	}
	return m
}
