/*
File Structures 2

This is a follow up to my crufty
http://github.com/timtadh/file-structures work. That system has some
endemic problems:

1. It uses the read/write interface to files. This means that it needs
to do block management and cache management. In theory this can be
very fast but it is also very challenging in Go.

2. Because it uses read/write it has to do buffer management.
Largely, the system punts on this problem and allows go to handle the
buffer management through the normal memory management system. This
doesn't work especially well for the use case of file-structures.

File Structures 2 is an experiment to bring Memory Mapped IO to the
world of Go. The hypotheses are:

1. The operating system is good at page management generally. While,
we know more about how to manage the structure of B+Trees, VarChar
stores, and Linear Hash tables than the OS there is no indication
that from Go you can acheive better performance. Therefore, I
hypothesize that leaving it to the OS will lead to a smaller working
set and a faster data structure in general.

2. You can make Memory Mapping performant in Go. There are many
challenges here. The biggest of which is that there are no
dynamically size array TYPES in go. The size of the array is part of
the type, you have to use slices. This creates complications when
hooking up structures which contain slices to mmap allocated blocks
of memory. I hypothesize that this repository can acheive good
(enough) performance here.

The major components of this project:

1. fmap - a memory mapped file inteface. Part C part Go. Uses cgo.

2. bptree - a B+ Tree with duplicate key support (fixed size keys,
variable length values) written on top of fmap.

3. slice - used by fmap and bptree to completely violate memory and
type safety of Go.

4. errors - just a simple error package which maintains a stack trace
with every error.

*/
package fs2


