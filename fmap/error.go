package fmap

import (
	"fmt"
	"runtime/debug"
)

type Error struct {
	Err error
	Stack []byte
}

func Errorf(format string, args ...interface{}) error {
	return &Error{
		Err: fmt.Errorf(format, args...),
		Stack: debug.Stack(),
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s\n%s", e.Err, string(e.Stack))
}

func (e *Error) String() string {
	return e.Error()
}

