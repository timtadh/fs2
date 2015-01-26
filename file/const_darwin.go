// +build darwin

package file

import "os"
import "fmt"
import "syscall"

const OPENFLAG = os.O_RDWR | os.O_CREAT

func (self *BlockFile) open() error {
	// the O_DIRECT flag turns off os buffering of pages allow us to do it manually
	// when using the O_DIRECT block size must be a multiple of 2048
	if f, err := os.Open(self.filename, OPENFLAG, 0666); err != nil {
		return err
	} else {
		self.file = f
		self.opened = true
		r1, r2, err := syscall.Syscall(syscall.SYS_FCNTL, uintptr(self.file.Fd()), syscall.F_NOCACHE, 1)
		if err != 0 {
			err := fmt.Errorf("Syscall to SYS_FCNTL failed\n\tr1=%v, r2=%v, err=%v\n", r1, r2, err)
			self.Close()
			return err
		}
	}
	return nil
}
