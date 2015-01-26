package file

import (
	"fmt"
	"hash/crc32"
	"os"
	"reflect"
	"unsafe"
)

import (
	"github.com/timtadh/fs2/slice"
)

const BLOCKSIZE = 4096

type freeblk struct {
	next uint64
}

func loadFreeBlk(bytes []byte) *freeblk {
	free_s := slice.AsSlice(&bytes)
	return (*freeblk)(free_s.Array)
}

type ctrldata struct {
	checksum  uint32
	blksize   uint32
	free_head uint64
	free_len  uint32
}

func (cd *ctrldata) Size() uintptr {
	return reflect.TypeOf(*cd).Size()
}

type ctrlblk struct {
	back []byte
	data *ctrldata
	user  []byte
}

func load_ctrlblk(bytes []byte) (cb *ctrlblk, err error) {
	back := slice.AsSlice(&bytes)
	data := (*ctrldata)(back.Array)
	ptr := uintptr(back.Array) + data.Size()
	new_chksum := crc32.ChecksumIEEE(bytes[4:])
	if new_chksum != data.checksum {
		return nil, fmt.Errorf("Bad control block checksum %x != %x", new_chksum, data.checksum)
	}
	user_len := len(bytes) - int(data.Size())
	user_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: user_len,
		Cap: user_len,
	}
	user := *user_s.AsBytes()
	cb = &ctrlblk{
		back: bytes,
		data: data,
		user: user,
	}
	return cb, nil
}

func new_ctrlblk(bytes []byte, blksize uint32) (cb *ctrlblk) {
	back := slice.AsSlice(&bytes)
	data := (*ctrldata)(back.Array)
	ptr := uintptr(back.Array) + data.Size()
	data.blksize = blksize
	data.free_head = 0
	data.free_len = 0
	user_len := len(bytes) - int(data.Size())
	user_s := &slice.Slice{
		Array: unsafe.Pointer(ptr),
		Len: user_len,
		Cap: user_len,
	}
	user := *user_s.AsBytes()
	copy(user, make([]byte, len(user))) // zeros the user data
	data.checksum = crc32.ChecksumIEEE(bytes[4:])
	cb = &ctrlblk{
		back: bytes,
		data: data,
		user: user,
	}
	return cb
}

func (cb *ctrlblk) Block() []byte {
	return cb.back
}

func (cb *ctrlblk) updateChkSum() {
	cb.data.checksum = crc32.ChecksumIEEE(cb.back[4:])
}

type BlockFile struct {
	path   string
	opened bool
	file   *os.File
	ctrl   *ctrlblk
}

func NewBlockFile(path string) *BlockFile {
	return NewBlockFileCustomBlockSize(path, BLOCKSIZE)
}

func NewBlockFileCustomBlockSize(path string, size uint32) *BlockFile {
	if size%4096 != 0 {
		panic(fmt.Errorf("blocksize must be divisible by 4096"))
	}
	cb := new_ctrlblk(make([]byte, size), size)
	return &BlockFile{
		path: path,
		ctrl: cb,
	}
}

func (self *BlockFile) Open() error {
	if err := self.open(); err != nil {
		return err
	}
	if size, err := self.Size(); err != nil {
		return err
	} else if size == 0 {
		if _, err := self.Allocate(); err != nil {
			return err
		} else {
			if err := self.write_ctrlblk(); err != nil {
				return err
			}
		}
	} else {
		if err := self.read_ctrlblk(); err != nil {
			return err
		}
	}
	return nil
}

func (self *BlockFile) Close() error {
	if err := self.file.Close(); err != nil {
		return err
	} else {
		self.file = nil
		self.opened = false
	}
	return nil
}

func (self *BlockFile) Remove() error {
	if self.opened {
		return fmt.Errorf("Expected file to be closed")
	}
	return os.Remove(self.Path())
}

func (self *BlockFile) write_ctrlblk() error {
	self.ctrl.updateChkSum()
	return self.WriteBlock(0, self.ctrl.Block())
}

func (self *BlockFile) read_ctrlblk() error {
	if bytes, err := self.ReadBlock(0); err != nil {
		return err
	} else {
		if cb, err := load_ctrlblk(bytes); err != nil {
			return err
		} else {
			self.ctrl = cb
		}
	}
	return nil
}

func (self *BlockFile) ControlData() (data []byte) {
	data = make([]byte, len(self.ctrl.user))
	copy(data, self.ctrl.user)
	return data
}

func (self *BlockFile) SetControlData(data []byte) (err error) {
	if len(data) > len(self.ctrl.user) {
		return fmt.Errorf("control data was too large")
	}
	copy(self.ctrl.user, data)
	return self.write_ctrlblk()
}

func (self *BlockFile) Path() string {
	return self.path
	}

func (self *BlockFile) BlockSize() uint32 {
	return self.ctrl.data.blksize
}

func (self *BlockFile) Size() (uint64, error) {
	if !self.opened {
		return 0, fmt.Errorf("File is not open")
	}
	dir, err := os.Stat(self.path)
	if err != nil {
		return 0, err
	}
	return uint64(dir.Size()), nil
}

func (self *BlockFile) resize(size int64) error {
	return self.file.Truncate(size)
}

func (self *BlockFile) Free(pos int64) error {
	head := self.ctrl.data.free_head
	free_bytes := make([]byte, self.ctrl.data.blksize)
	free := loadFreeBlk(free_bytes)
	free.next = head
	if err := self.WriteBlock(pos, free_bytes); err != nil {
		return err
	}
	self.ctrl.data.free_head = uint64(pos)
	self.ctrl.data.free_len += 1
	return self.write_ctrlblk()
}

func (self *BlockFile) pop_free() (pos int64, err error) {
	if self.ctrl.data.free_head == 0 && self.ctrl.data.free_len == 0 {
		return 0, fmt.Errorf("No blocks free")
	}
	pos = int64(self.ctrl.data.free_head)
	if bytes, err := self.ReadBlock(pos); err != nil {
		return 0, err
	} else {
		free := loadFreeBlk(bytes)
		self.ctrl.data.free_head = free.next
	}
	self.ctrl.data.free_len -= 1
	if err := self.write_ctrlblk(); err != nil {
		return 0, err
	}
	return pos, err
}

func (self *BlockFile) alloc(n int) (pos int64, err error) {
	var size uint64
	amt := uint64(self.ctrl.data.blksize) * uint64(n)
	if size, err = self.Size(); err != nil {
		return 0, err
	}
	if err := self.resize(int64(size + amt)); err != nil {
		return 0, err
	}
	return int64(size), nil
}

func (self *BlockFile) Allocate() (pos int64, err error) {
	if self.ctrl.data.free_len == 0 {
		return self.alloc(1)
	}
	return self.pop_free()
}

func (self *BlockFile) AllocateBlocks(n int) (pos int64, err error) {
	return self.alloc(n)
}

func (self *BlockFile) WriteBlock(p int64, block []byte) error {
	if !self.opened {
		return fmt.Errorf("File is not open")
	}
	for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
		if err != nil {
			return err
		}
	}
	n, err := self.file.Write(block)
	if err == nil && n != len(block) {
		return fmt.Errorf("could not write the full block")
	}
	return err
}

func (self *BlockFile) ReadInto(p int64, block []byte) (error) {
	if len(block) % int(self.ctrl.data.blksize) != 0 {
		return fmt.Errorf("block is not a multiple of the block size")
	}
	if !self.opened {
		return fmt.Errorf("File is not open")
	}
	for pos, err := self.file.Seek(p, 0); pos != p; pos, err = self.file.Seek(p, 0) {
		if err != nil {
			return err
		}
	}
	n, err := self.file.Read(block)
	if err == nil && n != len(block) {
		return fmt.Errorf("could not read the full block")
	}
	return err
}

func (self *BlockFile) ReadBlock(p int64) ([]byte, error) {
	block := make([]byte, self.ctrl.data.blksize)
	if err := self.ReadInto(p, block); err != nil {
		return nil, err
	}
	return block, nil
}

func (self *BlockFile) ReadBlocks(p int64, n int) ([]byte, error) {
	block := make([]byte, int(self.ctrl.data.blksize)*n)
	if err := self.ReadInto(p, block); err != nil {
		return nil, err
	}
	return block, nil
}

