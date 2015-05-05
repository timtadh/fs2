package bptree

import (
	"bytes"
)

import (
)

type keyed interface {
	key(i int) []byte
	doKeyAt(v *Varchar, i int, do func(key []byte) error) error
	keyCount() int
}

func find(v *Varchar, keys keyed, key []byte) (int, bool, error) {
	var l int = 0
	var r int = keys.keyCount() - 1
	var m int
	for l <= r {
		var cmp int
		m = ((r - l) >> 1) + l
		err := keys.doKeyAt(v, m, func(key_m []byte) error {
			cmp = bytes.Compare(key, key_m)
			return nil
		})
		if err != nil {
			return 0, false, err
		}
		if cmp < 0 {
			r = m - 1
		} else if cmp == 0 {
			for j := m; j >= 0; j-- {
				if j == 0 {
					return j, true, nil
				}
				var cmp int
				err := keys.doKeyAt(v, j-1, func(key_j_1 []byte) error {
					cmp = bytes.Compare(key, key_j_1)
					return nil
				})
				if err != nil {
					return 0, false, err
				}
				if cmp != 0 {
					return j, true, nil
				}
			}
		} else {
			l = m + 1
		}
	}
	return l, false, nil
}

func shift(bytes []byte, idx, length, amt int, left bool) {
	moving := bytes[idx : idx+length]
	var to []byte
	if left {
		to = bytes[idx+amt : idx+length+amt]
	} else {
		to = bytes[idx-amt : idx+length-amt]
	}
	copy(to, moving)
}
