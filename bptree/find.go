package bptree

import (
	"bytes"
	"log"
)

import (
	"github.com/timtadh/fs2/errors"
)

type keyed interface {
	key(i int) []byte
	doKeyAt(v *Varchar, i int, do func(key []byte) error) error
	cmpKeyAt(v *Varchar, i int, key []byte) (int, error)
	keyCount() int
	Debug(v *Varchar) string
}

func checkOrder(v *Varchar, n keyed) error {
	for i := 1; i < n.keyCount(); i++ {
		err := n.doKeyAt(v, i-1, func(k_0 []byte) error {
			return n.doKeyAt(v, i, func(k_1 []byte) error {
				if bytes.Compare(k_0, k_1) > 0 {
					log.Println("k_0", k_0)
					log.Println("k_1", k_1)
					return errors.Errorf("node was out of order %v %v %v", i-1, i, bytes.Compare(k_0, k_1))
				}
				return nil
			})
		})
		if err != nil {
			log.Println(n)
			log.Println(n.Debug(v))
			return err
		}
	}
	return nil
}

func find(v *Varchar, keys keyed, key []byte) (int, bool, error) {
	var l int = 0
	var r int = keys.keyCount() - 1
	var m int
	for l <= r {
		m = ((r - l) >> 1) + l
		cmp, err := keys.cmpKeyAt(v, m, key)
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
				cmp, err := keys.cmpKeyAt(v, j-1, key)
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
