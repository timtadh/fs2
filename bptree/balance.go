package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
)

func (a *internal) balance(b *internal) error {
	if b.meta.keyCount != 0 {
		return errors.Errorf("b was not empty")
	}
	var m int = a.balancePoint()
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		copy(b.key(i), a.key(j))
		fmap.MemClr(a.key(j))
		b.ptrs[i] = a.ptrs[j]
		a.ptrs[j] = 0
	}
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	return nil
}

func (a *leaf) balance(b *leaf) error {
	if b.meta.keyCount != 0 {
		return errors.Errorf("b was not empty")
	}
	var m int = a.balancePoint()
	if m == 0 {
		// we had a pure balance
		return nil
	}
	return a.balanceAt(b, m)
}

func (a *leaf) balanceAt(b *leaf, m int) error {
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		b.valueSizes[i] = a.valueSizes[j]
		a.valueSizes[j] = 0
		b.valueFlags[i] = a.valueFlags[j]
		a.valueFlags[j] = 0
	}
	m_offset := a.keyOffset(m)
	from := a.kvs[m_offset:]
	copy(b.kvs, from)
	fmap.MemClr(from)
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	return nil
}

func (n *internal) balancePoint() int {
	m := int(n.meta.keyCount) / 2
	return noSplitBalancePoint(n, m)
}

func (n *leaf) balancePoint() int {
	if n.meta.keyCount == 0 {
		return 0
	}
	length := n.next_kv_in_kvs()
	guess := length / 2
	m := 0
	for m+1 < int(n.meta.keyCount) {
		if n.keyOffset(m+1) > guess {
			break
		}
		m++
	}
	return noSplitBalancePoint(n, m)
}

func noSplitBalancePoint(keys keyed, m int) int {
	for m < keys.keyCount() && bytes.Equal(keys.key(m-1), keys.key(m)) {
		m++
	}
	if m >= keys.keyCount() && m > 0 {
		m--
		for m > 0 && bytes.Equal(keys.key(m-1), keys.key(m)) {
			m--
		}
	}
	return m
}

// merges b into a. after this method returns b will be empty
func (a *leaf) merge(b *leaf) error {
	if b.meta.keyCount == 0 {
		return errors.Errorf("b was empty")
	}
	swapped := false
	if bytes.Compare(a.key(0), b.key(0)) > 0 {
		a, b = b, a
		swapped = true
	}
	total := int(a.meta.keyCount) + int(b.meta.keyCount)
	if total > int(a.meta.keyCap) {
		return errors.Errorf("merge impossible")
	}
	for i := 0; i < int(b.meta.keyCount); i++ {
		j := int(a.meta.keyCount) + i
		a.valueSizes[j] = b.valueSizes[i]
		b.valueSizes[i] = 0
		a.valueFlags[j] = b.valueFlags[i]
		b.valueFlags[i] = 0
	}
	m_offset := a.keyOffset(int(a.meta.keyCount))
	to := a.kvs[m_offset:]
	copy(to, b.kvs)
	fmap.MemClr(b.kvs)
	b.meta.keyCount = 0
	a.meta.keyCount = uint16(total)
	if swapped {
		for i := 0; i < int(a.meta.keyCount); i++ {
			b.valueSizes[i] = a.valueSizes[i]
			a.valueSizes[i] = 0
			b.valueFlags[i] = a.valueFlags[i]
			a.valueFlags[i] = 0
		}
		copy(b.kvs, a.kvs)
		fmap.MemClr(a.kvs)
		b.meta.keyCount = a.meta.keyCount
		a.meta.keyCount = 0
	}
	return nil
}
