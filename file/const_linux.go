// +build linux

package file

import "os"
import "syscall"

/*
The O_DIRECT flag seems to lower performance. So I am turning it off. O_SYNC
flag REALLY lowers performance.
*/
var OPENFLAG = os.O_RDWR | os.O_CREATE | syscall.O_DIRECT | syscall.O_NOATIME // | os.O_SYNC

// var OPENFLAG = os.O_RDWR | os.O_CREATE | syscall.O_NOATIME

func (self *BlockFile) open() error {
	// the O_DIRECT flag turns off os buffering of pages allow us to do it manually
	// when using the O_DIRECT block size must be a multiple of 2048
	if f, err := os.OpenFile(self.path, OPENFLAG, 0666); err != nil {
		return err
	} else {
		self.file = f
		self.opened = true
	}
	return nil
}
