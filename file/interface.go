package file

type BlockSizer interface {
	BlockSize() uint32
}

type BlockReader interface {
	ReadBlock(key int64) (block []byte, err error)
	ReadBlocks(key int64, n int) (blocks []byte, err error)
}

type BlockWriter interface {
	WriteBlock(key int64, block []byte) error
}

type BlockReadWriter interface {
	BlockReader
	BlockWriter
}

type BlockAllocator interface {
	Free(key int64) error
	Allocate() (key int64, err error)
	AllocateBlocks(n int) (key int64, err error)
}

type Closer interface {
	Close() error
}

type Removable interface {
	Remove() error
}

type RootController interface {
	ControlData() (block []byte)
	SetControlData(block []byte) (err error)
}

type BlockDevice interface {
	BlockSizer
	BlockReadWriter
	BlockAllocator
	Closer
	RootController
}

type RemovableBlockDevice interface {
	BlockDevice
	Removable
}
