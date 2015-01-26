package bptree

const BLOCKSIZE = 4096

type Allocator func() []byte

func DefaultAllocator() []byte {
	return make([]byte, BLOCKSIZE)
}
