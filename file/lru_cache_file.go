package file

import (
	"container/list"
	"fmt"
)

type lru struct {
	buffer  map[int64]*list.Element
	stack   *list.List
	size    int
	pageout func(int64, []byte) error
}

type LRUCacheFile struct {
	file       RemovableBlockDevice
	cache      map[int64]*lru_item
	cache_size int
	lru        *lru
}

func NewLRUCacheFile(file RemovableBlockDevice, size uint64) (cf *LRUCacheFile, err error) {
	cache_size := 0
	if size > 0 {
		cache_size = 1 + int(size/uint64(file.BlockSize()))
	}
	cf = &LRUCacheFile{
		file:       file,
		cache:      make(map[int64]*lru_item),
		cache_size: cache_size,
	}
	cf.lru = newLRU(cache_size, cf.pageout)
	return cf, nil
}

func OpenLRUCacheFile(file RemovableBlockDevice, size uint64) (cf *LRUCacheFile, err error) {
	cache_size := 0
	if size > 0 {
		cache_size = 1 + int(size/uint64(file.BlockSize()))
	}
	cf = &LRUCacheFile{
		file:       file,
		cache:      make(map[int64]*lru_item),
		cache_size: cache_size,
	}
	cf.lru = newLRU(cache_size, cf.pageout)
	data := cf.file.ControlData()
	err = cf.SetControlData(data)
	if err != nil {
		return nil, err
	}
	return cf, nil
}

func (self *LRUCacheFile) Close() error {
	if err := self.file.Close(); err != nil {
		return err
	}
	return nil
}

func (self *LRUCacheFile) Remove() error {
	return self.file.Remove()
}

func (self *LRUCacheFile) Persist() error {
	err := self.lru.Persist()
	if err != nil {
		return err
	}
	return nil
}

func (self *LRUCacheFile) ControlData() (data []byte) {
	return self.file.ControlData()
}

func (self *LRUCacheFile) SetControlData(data []byte) (err error) {
	return self.file.SetControlData(data)
}

func (self *LRUCacheFile) BlockSize() uint32 { return self.file.BlockSize() }

func (self *LRUCacheFile) Free(key int64) error {
	self.lru.Remove(key)
	if err := self.file.Free(key); err != nil {
		return err
	}
	return nil
}

func (self *LRUCacheFile) Allocate() (key int64, err error) {
	return self.file.Allocate()
}

func (self *LRUCacheFile) AllocateBlocks(n int) (key int64, err error) {
	return self.file.AllocateBlocks(n)
}

func (self *LRUCacheFile) pageout(key int64, block []byte) error {
	return self.file.WriteBlock(key, block)
}

func (self *LRUCacheFile) WriteBlock(key int64, block []byte) (err error) {
	return self.lru.Update(key, block, false)
}

func (self *LRUCacheFile) ReadBlock(key int64) (block []byte, err error) {
	block, has := self.lru.Read(key, self.BlockSize())
	if !has {
		block, err := self.file.ReadBlock(key)
		if err != nil {
			return nil, err
		}
		err = self.lru.Update(key, block, true)
		if err != nil {
			return nil, err
		}
		return block, nil
	} else {
		return block, nil
	}
}

func (self *LRUCacheFile) ReadBlocks(key int64, n int) (blocks []byte, err error) {
	buffer_read := func() bool {
		ckey := key
		for i := 0; i < n; i++ {
			if self.lru.Has(ckey) {
				return true
			}
			ckey += int64(self.BlockSize())
		}
		return false
	}()

	if buffer_read {
		blk_size := int64(self.BlockSize())
		blocks = make([]byte, n*int(blk_size))
		for i := int64(0); i < int64(n); i++ {
			blk, err := self.ReadBlock(key + i*blk_size)
			if err != nil {
				return nil, err
			}
			copy(blocks[i*blk_size:(i+1)*blk_size], blk)
		}
		return blocks, nil
	} else {
		return self.file.ReadBlocks(key, n)
		// we aren't saving it the cache on purpose. This could read a bunch of one use
		// blocks
	}
}

// -------------------------------------------------------------------------------------

type lru_item struct {
	bytes []byte
	p     int64
	dirty bool
}

func new_lruitem(p int64, bytes []byte) *lru_item {
	return &lru_item{
		p:     p,
		bytes: bytes,
		dirty: true,
	}
}

func newLRU(size int, pageout func(int64, []byte) error) *lru {
	self := new(lru)
	self.buffer = make(map[int64]*list.Element)
	self.stack = list.New()
	self.size = size - 1
	self.pageout = pageout
	return self
}

func (self *lru) Size() int { return self.size }

func (self *lru) Remove(p int64) {
	self.Update(p, nil, false)
}

func (self *lru) Persist() error {
	for self.stack.Len() > 0 {
		e := self.stack.Back()
		if e == nil {
			return fmt.Errorf("Element unexpectedly nil %v", self.stack.Len())
		}
		i := e.Value.(*lru_item)
		if i.dirty {
			err := self.pageout(i.p, i.bytes)
			if err != nil {
				return err
			}
		}
		delete(self.buffer, i.p)
		self.stack.Remove(e)
	}
	return nil
}

func (self *lru) Has(p int64) bool {
	_, has := self.buffer[p]
	return has
}

func (self *lru) Update(p int64, block []byte, fromdisk bool) error {
	if e, has := self.buffer[p]; has {
		if block == nil {
			delete(self.buffer, p)
			self.stack.Remove(e)
		} else {
			item := e.Value.(*lru_item)
			item.bytes = block
			item.dirty = true
			self.stack.MoveToFront(e)
		}
	} else {
		if block == nil {
			// deleting the block, and it isn't in the cache
			// so do nothing.
			return nil
		}
		for self.size < self.stack.Len() && self.stack.Len() > 0 {
			e = self.stack.Back()
			if e == nil {
				return fmt.Errorf("Element unexpectedly nil %v", self.stack.Len())
			}
			i := e.Value.(*lru_item)
			if i.dirty {
				err := self.pageout(i.p, i.bytes)
				if err != nil {
					return err
				}
			}
			delete(self.buffer, i.p)
			self.stack.Remove(e)
		}
		item := new_lruitem(p, block)
		if fromdisk {
			item.dirty = false
		}
		e = self.stack.PushFront(item)
		if e == nil {
			return fmt.Errorf("Element unexpectedly nil on insert")
		}
		self.buffer[p] = e
	}
	return nil
}

func (self *lru) Read(p int64, length uint32) ([]byte, bool) {
	if e, has := self.buffer[p]; has {
		if i, ok := e.Value.(*lru_item); ok {
			if len(i.bytes) != int(length) {
				return nil, false
			}
			self.stack.MoveToFront(e)
			// hit
			return i.bytes, true
		}
	}
	// miss
	return nil, false
}
