package bptree

import (
	"bytes"
	"log"
)

import (
	"github.com/timtadh/fs2/consts"
	"github.com/timtadh/fs2/errors"
)

// Verify() error
// Looks at the structure of the B+Tree and checks it conforms to the B+Tree structural
// invariants. It can be used to look check for database corruption either from errors
// in the algorithms or from disk or memory corruption. It should be noted that it
// cannot check to ensure that no bits have flipped inside of keys and values as
// currently not error correcting codes are generated for them. This only looks at the
// structure of the tree itself. It could be corruption has occurred and this will not
// find it as the tree is still a valid B+Tree.
func (self *BpTree) Verify() (err error) {
	return self.verify(0, 0, self.meta.root, 0)
}

func (self *BpTree) verify(parent uint64, idx int, n, sibling uint64) (err error) {
	var flags consts.Flag
	err = self.bf.Do(n, 1, func(bytes []byte) error {
		flags = consts.AsFlag(bytes)
		return nil
	})
	if err != nil {
		return err
	}
	if flags&consts.INTERNAL != 0 {
		return self.internalVerify(parent, idx, n, sibling)
	} else if flags&consts.LEAF != 0 {
		return self.leafVerify(parent, idx, n, sibling)
	} else {
		return errors.Errorf("Unknown block type")
	}
}

func (self *BpTree) internalVerify(parent uint64, idx int, n, sibling uint64) (err error) {
	a := n
	return self.doInternal(a, func(n *internal) error {
		err := checkOrder(self.varchar, n)
		if err != nil {
			log.Println("error in internalVerify")
			log.Printf("out of order")
			log.Println("internal", a, n.Debug(self.varchar))
			return err
		}
		for i := 0; i < n.keyCount(); i++ {
			var sib uint64
			if i+1 < n.keyCount() {
				sib = *n.ptr(i + 1)
			} else if sibling != 0 {
				self.doInternal(sibling, func(sn *internal) error {
					sib = *sn.ptr(0)
					return nil
				})
			}
			err := self.verify(a, i, *n.ptr(i), sib)
			if err != nil {
				log.Println("------------------- internal -------------------")
				log.Println("error in internalVerify")
				log.Printf("could not verify node. failed kid %v", i)
				log.Println("n", a, n.Debug(self.varchar))
				if sibling != 0 {
					self.doInternal(sibling, func(n *internal) error {
						log.Println("sibing", sibling, n.Debug(self.varchar))
						return nil
					})
				}
				if parent != 0 {
					self.doInternal(parent, func(n *internal) error {
						log.Println("parent", parent, n.Debug(self.varchar))
						return nil
					})
				}
				log.Printf("n = %v, sibling = %v, parent = %v, parent idx = %v, i = %v, sib = %v", a, sibling, parent, idx, i, sib)
				return err
			}
		}
		return nil
	})
}

func (self *BpTree) leafVerify(parent uint64, idx int, n, sibling uint64) (err error) {
	a := n
	return self.doLeaf(a, func(n *leaf) error {
		if n.keyCount() == 0 {
			if parent != 0 {
				log.Println("warn, keyCount == 0", a, parent, sibling)
			}
			return nil
		}
		if n.pure(self.varchar) {
			return self.pureVerify(parent, idx, a, sibling)
		}
		err := self.leafOrderVerify(parent, idx, a, sibling)
		if err != nil {
			log.Println("error in leafVerify")
			log.Printf("out of order")
			log.Println("leaf", a, n.Debug(self.varchar))
			return err
		}
		if n.meta.next != sibling {
			log.Println("error in leafVerify")
			log.Println("n.meta.next != sibling", n.meta.next, sibling)
			self.doLeaf(n.meta.next, func(m *leaf) error {
				log.Println("a", a, n.Debug(self.varchar))
				log.Println("a.meta.next", n.meta.next, m.Debug(self.varchar))
				return self.doLeaf(sibling, func(o *leaf) error {
					log.Println("sibling", sibling, o.Debug(self.varchar))
					return nil
				})
			})
			return errors.Errorf("n.meta.next (%v) != sibling (%v)", n.meta.next, sibling)
		}
		return nil
	})
}

func (self *BpTree) pureVerify(parent uint64, idx int, n, sibling uint64) (err error) {
	a := n
	return self.doLeaf(a, func(n *leaf) error {
		run, err := self.pureRun(a)
		if err != nil {
			log.Println("error in pureVerify")
			log.Println("end of pure run error")
			log.Println("leaf", a, n.Debug(self.varchar))
			return err
		}
		for i, x := range run {
			err := self.leafOrderVerify(parent, idx, x, sibling)
			if err != nil {
				log.Println("error in pureVerify")
				log.Printf("leafOrderVerify failed for run item %v", i)
				log.Println("start of run", a, n.Debug(self.varchar))
				self.doLeaf(x, func(o *leaf) error {
					log.Println("cur item of run", x, o.Debug(self.varchar))
					return nil
				})
				return err
			}
		}
		e := run[len(run)-1]
		return self.doLeaf(e, func(m *leaf) (err error) {
			err = checkOrder(self.varchar, m)
			if err != nil {
				log.Println("error in pureVerify")
				log.Printf("e out of order")
				log.Println("leaf", e, n.Debug(self.varchar))
				return err
			}
			err = n.doKeyAt(self.varchar, 0, func(a_key_0 []byte) error {
				return m.doKeyAt(self.varchar, 0, func(e_key_0 []byte) error {
					if !bytes.Equal(a_key_0, e_key_0) {
						log.Println("a", a, n.Debug(self.varchar))
						log.Println("e", e, m.Debug(self.varchar))
						log.Println("went off of end of pure run")
						return errors.Errorf("End of pure run went off of pure run")
					}
					if m.meta.next == 0 {
						return nil
					}
					return self.doLeaf(m.meta.next, func(o *leaf) error {
						return o.doKeyAt(self.varchar, 0, func(o_key_0 []byte) error {
							if bytes.Equal(a_key_0, o_key_0) {
								log.Println("a", a, n.Debug(self.varchar))
								log.Println("e", e, m.Debug(self.varchar))
								log.Println("e.meta.next", m.meta.next, o.Debug(self.varchar))
								log.Println("did not find end of pure run")
								return errors.Errorf("did not find end of pure run")
							}
							return nil
						})
					})
				})
			})
			if err != nil {
				log.Println("error in pureVerify")
				return err
			}
			if m.meta.next != sibling {
				log.Println("error in pureVerify")
				log.Println("m.meta.next != sibling", m.meta.next, sibling)
				self.doLeaf(m.meta.next, func(o *leaf) error {
					log.Println("a", a, n.Debug(self.varchar))
					log.Println("e", e, m.Debug(self.varchar))
					log.Println("e.meta.next", m.meta.next, o.Debug(self.varchar))
					return nil
				})
				return errors.Errorf("m.meta.next (%v) != sibling (%v)", m.meta.next, sibling)
			}
			return nil
		})
	})
}

func (self *BpTree) leafOrderVerify(parent uint64, idx int, n, sibling uint64) (err error) {
	a := n
	return self.doLeaf(a, func(n *leaf) error {
		if n.keyCount() == 0 {
			if parent != 0 {
				log.Println("warn, keyCount == 0", a, parent, sibling)
			}
			return nil
		}
		err := checkOrder(self.varchar, n)
		if err != nil {
			log.Println("error in leafVerify")
			log.Printf("out of order")
			log.Println("leaf", a, n.Debug(self.varchar))
			return err
		}
		if n.meta.next != 0 {
			err = n.doKeyAt(self.varchar, n.keyCount()-1, func(last []byte) error {
				return self.doLeaf(n.meta.next, func(m *leaf) error {
					if m.keyCount() == 0 {
						log.Println("a.meta.next has no keys", n.meta.next)
						log.Println("a", a, n.Debug(self.varchar))
						log.Println("a.meta.next", n.meta.next, m.Debug(self.varchar))
						return nil
					}
					return m.doKeyAt(self.varchar, 0, func(first []byte) error {
						cmp := bytes.Compare(last, first)
						if cmp > 0 {
							log.Println("a", a, n.Debug(self.varchar))
							log.Println("a.meta.next", n.meta.next, m.Debug(self.varchar))
							log.Printf("last %v", last)
							log.Printf("first %v", first)
							log.Println("last of a is greater than first of a.meta.next")
							return errors.Errorf("last of a is greater than first of a.meta.next")
						}
						return nil
					})
				})
			})
			if err != nil {
				return err
			}
		}
		if n.meta.prev != 0 {
			err = n.doKeyAt(self.varchar, 0, func(first []byte) error {
				return self.doLeaf(n.meta.prev, func(m *leaf) error {
					if m.keyCount() == 0 {
						log.Println("a.meta.prev has no keys")
						log.Println("a", a, n.Debug(self.varchar))
						log.Println("a.meta.prev", n.meta.prev, m.Debug(self.varchar))
						return nil
					}
					return m.doKeyAt(self.varchar, m.keyCount()-1, func(last []byte) error {
						cmp := bytes.Compare(last, first)
						if cmp > 0 {
							log.Println("a.meta.prev", n.meta.prev, m.Debug(self.varchar))
							log.Println("a", a, n.Debug(self.varchar))
							log.Printf("last %v", last)
							log.Printf("first %v", first)
							log.Println("last of a.meta.prev is greater than first of a")
							return errors.Errorf("last of a.meta.prev is greater than first of a")
						}
						return nil
					})
				})
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
