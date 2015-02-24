/*
File memory MAP

The fmap package implements a nice way to memory map a file for block
oriented operations. It's interface is block oriented and allows you to
allocate and free blocks inside of the file. To support resizing the
underlying file outstanding pointers are tracked and are expected to be
released. This is done through run time checking.

*/
package fmap
