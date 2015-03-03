package bptree

import (
	"bytes"
)

type keyed interface {
	key(i int) []byte
	keyCount() int
}

func find(keys keyed, key []byte) (int, bool) {
	var l int = 0
	var r int = keys.keyCount() - 1
	var m int
	for l <= r {
		m = ((r - l) >> 1) + l
		cmp := bytes.Compare(key, keys.key(m))
		if cmp < 0 {
			r = m - 1
		} else if cmp == 0 {
			for j := m; j >= 0; j-- {
				if j == 0 || bytes.Compare(key, keys.key(j-1)) != 0 {
					return j, true
				}
			}
		} else {
			l = m + 1
		}
	}
	return l, false
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
