package bptree


func (self *BpTree) Put(key, value []byte) error {
	if len(key) != int(b.meta.keySize) {
		return Errorf("Key was not the correct size got, %v, expected, %v", len(key), b.meta.keySize)
	}
	a, b, err := self.insert(key, value)
	if err != nil {
		return err
	} else if b == nil {
		self.meta.root = a
		return self.writeMeta()
	}
	root, err := self.newInternal()
	if err != nil {
		return err
	}
	err = self.doInternal(root, func(n *internal) error {
		// TIM YOU ARE HERE
		// This function needs to do the same thing
		// as BpNode.put() in data-structures
		// You will need to have a way to extract
		// the first key from a block reguardless of
		// leaf, bigLeaf or internal.
	})
	if err != nil {
		return err
	}
	self.meta.root = root
	return root, nil
}

func (self *BpTree) doInternal(a uint64, do func(*internal) error) error {
	return self.bf.Do(a, 1, func(bytes []byte) error {
		n, err := loadInternal(bytes)
		if err != nil {
			return err
		}
		return do(n)
	})
}

func (self *BpTree) newInternal() (a uint64, err error) {
	a, err = self.bf.Allocate()
	if err != nil {
		return 0, err
	}
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		_, err := newInternal(bytes, self.meta.keySize)
		return err
	}
	if err != nil {
		return 0, err
	}
	return a, nil
}

