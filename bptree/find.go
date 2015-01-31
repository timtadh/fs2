package bptree

import (
	"bytes"
)

func find(keyCount int, keys [][]byte, key []byte) (int, bool) {
	var l int = 0
	var r int = keyCount - 1
	var m int
	for l <= r {
		m = ((r - l) >> 1) + l
		cmp := bytes.Compare(key, keys[m])
		if cmp < 0 {
			r = m - 1
		} else if cmp == 0 {
			for j := m; j >= 0; j-- {
				if j == 0 || bytes.Compare(key, keys[j-1]) != 0 {
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
	moving := bytes[idx:idx+length]
	var to []byte
	if left {
		to = bytes[idx+amt:idx+length+amt]
	} else {
		to = bytes[idx-amt:idx+length-amt]
	}
	copy(to, moving)
}

func putKey(keyCount int, keys [][]byte, key []byte, put func(i int) error) error {
	if keyCount + 1 >= len(keys) {
		return Errorf("Block is full.")
	}
	i, _ := find(keyCount, keys, key)
	if i < 0 {
		return Errorf("find returned a negative int")
	} else if i >= len(keys) {
		return Errorf("find returned a int > than cap(keys)")
	}
	if err := putItemAt(keyCount, keys, key, i); err != nil {
		return err
	}
	return put(i)
}

func putItemAt(itemCount int, items [][]byte, item []byte, i int) error {
	if itemCount == len(items) {
		return Errorf("The items slice is full")
	}
	if i < 0 || i >= len(items) {
		return Errorf("i was not in range")
	}
	for j := itemCount + 1; j > i; j-- {
		copy(items[j], items[j-1])
	}
	copy(items[i], item)
	return nil
}

