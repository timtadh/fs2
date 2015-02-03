package bptree

func (self *BpTree) firstKey(a uint64, do func(key []byte) error) error {
	return self.do(
		a,
		func(n *internal) error {
			if int(n.meta.keyCount) == 0 {
				return Errorf("Block was empty")
			}
			return do(n.keys[0])
		},
		func(n *leaf) error {
			if int(n.meta.keyCount) == 0 {
				return Errorf("Block was empty")
			}
			return do(n.keys[0])
		},
		func(n *bigLeaf) error {
			if int(n.meta.keyCount) == 0 {
				return Errorf("Block was empty")
			}
			return do(n.key)
		},
	)
}

