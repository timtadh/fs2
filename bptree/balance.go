package bptree

import (
	"bytes"
)

import (
	"github.com/timtadh/fs2/errors"
	"github.com/timtadh/fs2/fmap"
)

func (a *internal) balance(v *Varchar, b *internal) error {
	if b.meta.keyCount != 0 {
		return errors.Errorf("b was not empty")
	}
	m, err := a.balancePoint(v)
	if err != nil {
		return err
	}
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		copy(b.key(i), a.key(j))
		fmap.MemClr(a.key(j))
		*b.ptr(i) = *a.ptr(j)
		*a.ptr(j) = 0
	}
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	/*
		err = checkOrder(v, a)
		if err != nil {
			log.Println("balance point", m)
			log.Println(a)
			return err
		}
		err = checkOrder(v, b)
		if err != nil {
			log.Println("balance point", m)
			log.Println(b)
			return err
		}
	*/
	return nil
}

func (a *leaf) balance(v *Varchar, b *leaf) error {
	if b.meta.keyCount != 0 {
		return errors.Errorf("b was not empty")
	}
	m, err := a.balancePoint(v)
	if err != nil {
		return err
	}
	if m == 0 {
		// we had a pure balance
		return nil
	}
	err = a.balanceAt(b, m)
	if err != nil {
		return err
	}
	/*
		err = checkOrder(v, a)
		if err != nil {
			log.Println("balance point", m)
			log.Println(a)
			return err
		}
		err = checkOrder(v, b)
		if err != nil {
			log.Println("balance point", m)
			log.Println(b)
			return err
		}
	*/
	return nil
}

func (a *leaf) balanceAt(b *leaf, m int) error {
	var lim int = int(a.meta.keyCount) - m
	for i := 0; i < lim; i++ {
		j := m + i
		copy(b.val(i), a.val(j))
		fmap.MemClr(a.val(j))
		copy(b.key(i), a.key(j))
		fmap.MemClr(a.key(j))
	}
	b.meta.keyCount = a.meta.keyCount - uint16(m)
	a.meta.keyCount = uint16(m)
	return nil
}

func (n *internal) balancePoint(v *Varchar) (int, error) {
	m := int(n.meta.keyCount) / 2
	return noSplitBalancePoint(v, n, m)
}

func (n *leaf) balancePoint(v *Varchar) (int, error) {
	if n.meta.keyCount == 0 {
		return 0, nil
	}
	m := int(n.meta.keyCount) / 2
	return noSplitBalancePoint(v, n, m)
}

/*
func noSplitBalancePoint(keys keyed, m int) (int, error) {
	for m < keys.keyCount() && bytes.Equal(keys.key(m-1), keys.key(m)) {
		m++
	}
	if m >= keys.keyCount() && m > 0 {
		m--
		for m > 0 && bytes.Equal(keys.key(m-1), keys.key(m)) {
			m--
		}
	}
	return m, nil
}
*/

func noSplitBalancePoint(v *Varchar, keys keyed, m int) (int, error) {
	var eq bool
	for m < keys.keyCount() {
		err := keys.doKeyAt(v, m-1, func(a []byte) error {
			return keys.doKeyAt(v, m, func(b []byte) error {
				eq = bytes.Equal(a, b)
				return nil
			})
		})
		if err != nil {
			return 0, err
		}
		if !eq {
			break
		}
		m++
	}
	if m >= keys.keyCount() && m > 0 {
		m--
		for m > 0 {
			err := keys.doKeyAt(v, m-1, func(a []byte) error {
				return keys.doKeyAt(v, m, func(b []byte) error {
					eq = bytes.Equal(a, b)
					return nil
				})
			})
			if err != nil {
				return 0, err
			}
			if !eq {
				break
			}
			m--
		}
	}
	return m, nil
}

/*** outdated. Needs to be updated for doKeyAt

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
		copy(a.val(j), b.val(i))
		fmap.MemClr(b.val(i))
		copy(a.key(j), b.key(i))
		fmap.MemClr(b.key(i))
	}
	b.meta.keyCount = 0
	a.meta.keyCount = uint16(total)
	if swapped {
		for i := 0; i < int(a.meta.keyCount); i++ {
			copy(b.val(i), a.val(i))
			fmap.MemClr(a.val(i))
			copy(b.key(i), a.key(i))
			fmap.MemClr(a.key(i))
		}
		b.meta.keyCount = a.meta.keyCount
		a.meta.keyCount = 0
	}
	return nil
}

***/
