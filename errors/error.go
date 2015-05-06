package errors

import (
	"fmt"
	// "runtime/debug"
	"runtime"
)

type Error struct {
	Err   error
	Stack []byte
}

func Errorf(format string, args ...interface{}) error {
	buf := make([]byte, 50000)
	n := runtime.Stack(buf, false)
	trace := make([]byte, n)
	copy(trace, buf)
	return &Error{
		Err:   fmt.Errorf(format, args...),
		Stack: trace,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s\n%s", e.Err, string(e.Stack))
}

func (e *Error) String() string {
	return e.Error()
}
