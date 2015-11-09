package fmap

import "testing"

import (
	"runtime/debug"
)

var path string = "/tmp/__mmap_bf"

type T testing.T

func (t *T) assert(errors ...error) {
	for _, err := range errors {
		if err != nil {
			t.Log(string(debug.Stack()))
			t.Fatal(err)
		}
	}
}

func (t *T) blkfile() *BlockFile {
	bf, err := CreateBlockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bf
}

func (t *T) cleanup(bf *BlockFile) {
	err := bf.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = bf.Remove()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateBlockFile(t *testing.T) {
	bf, err := CreateBlockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	err = bf.ctrl(func(cb *ctrlblk) error {
		// if cb.meta.checksum == 0 {
		// t.Errorf("No checksum")
		// }
		if cb.meta.blksize != 4096 {
			t.Errorf("Blocksize was not 4096")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = bf.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = bf.Remove()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenBlockFile(t *testing.T) {
	{
		bf, err := CreateBlockFile(path)
		if err != nil {
			t.Fatal(err)
		}
		err = bf.ctrl(func(cb *ctrlblk) error {
			// if cb.meta.checksum == 0 {
			// t.Errorf("No checksum")
			// }
			if cb.meta.blksize != 4096 {
				t.Errorf("Blocksize was not 4096")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		err = bf.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	bf, err := OpenBlockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	err = bf.ctrl(func(cb *ctrlblk) error {
		// if cb.meta.checksum == 0 {
		// t.Errorf("No checksum")
		// }
		if cb.meta.blksize != 4096 {
			t.Errorf("Blocksize was not 4096")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = bf.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = bf.Remove()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAllocate(x *testing.T) {
	t := (*T)(x)
	bf := t.blkfile()
	defer t.cleanup(bf)
	var off uint64
	var err error
	off, err = bf.Allocate()
	t.assert(err)
	// var size uint64
	// size, err = bf.Size()
	// t.assert(err)
	// if size != uint64(bf.BlockSize())*2 {
	// t.Errorf("Size of the file did not increase")
	// }
	t.assert(bf.Do(off, 1, func(bytes []byte) error {
		bytes[15] = 12
		return nil
	}))
	t.assert(bf.Do(off, 1, func(bytes []byte) error {
		for i, b := range bytes {
			if i == 15 && b != 12 {
				t.Errorf("bytes[15] != 12")
			} else if i != 15 && b != 0 {
				t.Errorf("bytes[%d] != 0", i)
			}
		}
		return nil
	}))
}

func TestFree(x *testing.T) {
	t := (*T)(x)
	bf, err := Anonymous(4096)
	t.assert(err)
	var off uint64
	off, err = bf.Allocate()
	t.assert(err)
	// var size uint64
	// size, err = bf.Size()
	// t.assert(err)
	// if size != uint64(bf.BlockSize())*2 {
	// t.Errorf("Size of the file did not increase")
	// }
	t.assert(bf.Do(off, 1, func(bytes []byte) error {
		bytes[15] = 12
		return nil
	}))
	t.assert(bf.Do(off, 1, func(bytes []byte) error {
		for i, b := range bytes {
			if i == 15 && b != 12 {
				t.Errorf("bytes[15] != 12")
			} else if i != 15 && b != 0 {
				t.Errorf("bytes[%d] != 0", i)
			}
		}
		return nil
	}))
	t.assert(bf.Free(off))

	var off2 uint64
	off2, err = bf.Allocate()
	t.assert(err)
	if off2 != off {
		t.Errorf("off2 != off")
	}
	t.assert(bf.Do(off2, 1, func(bytes []byte) error {
		for i, b := range bytes {
			if b != 0 {
				t.Errorf("bytes[%d] != 0", i)
			}
		}
		return nil
	}))
}
