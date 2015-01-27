package bptree

import (
	"bytes"
	"fmt"
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

func putKey(keyCount, keys [][]byte, key []byte, put func(i int) error) error {
	if keyCount + 1 >= len(keys) {
		return fmt.Errorf("Block is full.")
	}
	i, _ := find(keyCount, keys, key)
	if i < 0 {
		panic(fmt.Errorf("find returned a negative int"))
	} else if i >= len(self.keys) {
		panic(fmt.Errorf("find returned a int > than cap(keys)"))
	}
	if err := putItemAt(keyCount, keys, key, i); err != nil {
		return err
	}
	return put(i)
}

func putItemAt(itemCount int, items [][]byte, item []byte, i int) error {
	if itemCount == len(items) {
		return fmt.Errorf("The items slice is full")
	}
	if i < 0 || i >= itemCount {
		return fmt.Errorf("i was not in range")
	}
	for j := itemCount + 1; j > i; j-- {
		copy(items[j], items[j-1])
	}
	copy(items[i], item)
	return nil
}

