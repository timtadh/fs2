# fs2 - File Structures 2

by Tim Henderson (tadh@case.edu)

Licensed under the GNU GPL version 3 or at your option any later version. If you
need another licensing option please contact me directly.

### What is this?

1. A [B+ Tree](#b-tree) implementation
2. A [list](#mmlist) implemenation supporting O(1) Append, Pop, Get and Set operations.
3. A [command](#fs2-generic) to generate type specific wrappers around the above
   structures. It's generic, in Go, kinda.
4. A [platform](#fmap) for implementing memory mapped high performance file
   structures in Go.

### Why did you make this?

In my academic research some of the algorithms I work on (such as frequent
subgraph mining) have exponential characteristics in their memory usage. In
order to run these algorithms on larger data sets they need to be able to
transparently cache less popular parts of the data to disk. However, in general
it is best to keep as much data in memory as possible.

Of course, there are many options for such data stores. I could have used a off
the shelf database, however I also want to explore ways to achieve higher
performance than those solutions offer.

#### Have you worked on this type of system before?

I have! In the golang world I believe I was the first to implement a disk backed
B+ Tree. Here is an [early
commit](https://github.com/timtadh/file-structures/commit/aedff4a077e16eb87e2d0f8ed4bc676debf7c572)
from  my [file-structures
repository](https://github.com/timtadh/file-structures). Note the date: February
21, 2010. Go was made public in November of 2009 (the first weekly was November
06, 2009). I started work on the B+ Tree in January of 2010.

This particular experiment is a follow up to my work in the file-structures
repository. I have used those structures successfully many times but I want to
experiment with new ways of doing things to achieve better results. Besides my
disk backed versions of these structures you can also find good implementations
of in memory version in my
[data-structures repository](https://github.com/timtadh/data-structures).

## B+ Tree

[docs](https://godoc.org/github.com/timtadh/fs2/bptree)

This is a disk backed low level "key-value store". The closest thing similar to
what it offers is [Bolt DB](https://github.com/boltdb/bolt).  My [blog
post](http://hackthology.com/lessons-learned-while-implementing-a-btree.html)
is a great place to start to learn more about the ubiquitous B+ Tree.

### Features

1. Variable length size key or fixed sized keys. Fixed sized keys should be kept
   relatively short, less than 1024 bytes (the shorter the better). Variable
   length keys can be up to 2^31 - 1 bytes long.

2. Variable length values or fixed sized values. Fixed sized values should also
   be kept short, less than 1024 bytes. Variable length values can be up to
   2^31 - 1 bytes long.

3. Duplicate key support. Duplicates are kept out of the index and only occur in
   the leaves.

4. Data is only written to disk when you tell it (or when need due to OS level
   page management).

5. Simple (but low level) interface.

6. Can operate in either a anonymous memory map or in a file backed memory map.
   If you plan to have a very large tree (even one that never needs to be
   persisted) it is recommend you use a file backed memory map. The OS treats
   pages in the file cache different than pages which are not backed by files.

7. The command `fs2-generic` can generate a wrapper specialized to your data
   type. Typing saved! To use `go install github.com/timtadh/fs2/fs2-generic`.
   Get help with `fs2-generic --help`

### Limitations

1. Not thread safe and therefore no transactions which you only need with
   multiple threads.

2. Maximum fixed key/value size is ~1350 bytes.

3. Maximum variable length key/value size is 2^31 - 1

4. This is not a database. You could make it into a database or build a database
   on top of it.

### Quick Start

[usage docs on godoc](https://godoc.org/github.com/timtadh/fs2/bptree#BpTree)

Importing

```go
import (
	"github.com/timtadh/fs2/bptree"
	"github.com/timtadh/fs2/fmap"
)
```

Creating a new B+ Tree (fixed key size, variable length value size).

```go
bf, err := fmap.CreateBlockFile("/path/to/file")
if err != nil {
	log.Fatal(err)
}
defer bf.Close()
bpt, err := bptree.New(bf, 8, -1)
if err != nil {
	log.Fatal(err)
}
// do stuff with bpt
```

Opening a B+ Tree

```go
bf, err := fmap.OpenBlockFile("/path/to/file")
if err != nil {
	log.Fatal(err)
}
defer bf.Close()
bpt, err := bptree.Open(bf)
if err != nil {
	log.Fatal(err)
}
// do stuff with bpt
```

Add a key/value pair. Note, since this is low level you have to serialize your
keys and values. The length of the []byte representing the key must exactly
match the key size of the B+ Tree. You can find out what that was set to by
called `bpt.KeySize()`

```go
import (
	"encoding/binary"
)

var key uint64 = 12
value := "hello world"
kBytes := make([]byte, 8)
binary.PutUvarint(kBytes, key)
err := bpt.Add(kBytes, []byte(value))
if err != nil {
	log.Fatal(err)
}
```

As you can see it can be a little verbose to serialize and de-serialize your
keys and values. So be sure to wrap that up in utility functions or even to wrap
the interface of the B+ Tree so that client code does not have to think about
it.

Since a B+Tree is a "multi-map" meaning there may be more than one value per
key. There is no "Get" method. To retrieve the values associated with a key use
the `Find` method.

```go
{
	var key, value []byte
	kvi, err := bpt.Find(kBytes)
	if err != nil {
		log.Fatal(err)
	}
	for key, value, err, kvi = kvi(); kvi != nil; key, value, err, kvi = kvi() {
		// do stuff with the keys and values
	}
	if err != nil {
		log.Fatal(err)
	}
}
```

That interface is easy to misuse if you do not check the error values as show in
the example above. An easier interface is provided for all of the iterators
(Range, Find, Keys, Values, Iterate) called the Do interface.

```go
err = bpt.DoFind(kBytes, func(key, value []byte) error {
	// do stuff with the keys and values
	return nil
})
if err != nil {
	log.Fatal(err)
}
```

It is recommended that you always use the Do\* interfaces. The other is provided
if the cost of extra method calls is too high.

Removal is also slightly more complicated due to the duplicate keys.  This
example will remove all key/value pairs associated with the given key:

```go
err = bpt.Remove(kBytes, func(value []byte) bool {
	return true
})
if err != nil {
	log.Fatal(err)
}
```

to remove just the one I added earlier do:

```go
err = bpt.Remove(kBytes, func(v []byte) bool {
	return bytes.Equal(v, []byte(value))
})
if err != nil {
	log.Fatal(err)
}
```

That wraps up the basic usage. If you want to ensure that the bytes you have
written are in fact on disk you have 2 options

1. call bf.Sync() - Note this uses the async mmap interface under the hood. The
   bytes are not guaranteed to hit the disk after this returns but they will go
   there soon.

2. call bf.Close()


## MMList

[docs](https://godoc.org/github.com/timtadh/fs2/mmlist)

A Memory Mapped List. This list works more like a stack and less like a queue.
It is not a good thing to build a job queue on. It is a good thing to build a
large set of items which can be efficiently randomly sampled. It uses the same
`varchar` system that the B+Tree uses so it can store variably sized items up to
2^31 - 1 bytes long.

Operations

1. `Size` O(1)
2. `Append` O(1)
3. `Pop` O(1)
4. `Get` O(1)
5. `Set` O(1)
6. `Swap` O(1)
7. `SwapDelete` O(1)

I will consider implementing a `Delete` method. However, it will be `O(n)` since
this is implemented a bit like an `ArrayList` under the hood. The actual way it
works is there is a B+Tree which indexes to list index blocks. The list index
blocks hold pointers (511 of them) to varchar locations. I considered having a
restricted 2 level index but that would have limited the size of the list to a
maximum of ~1 billion items which was uncomfortably small to me. In the future
the implementation may change to use something more like an ISAM index which
will be a bit more compact for this use case.

### Quickstart

```go
package main

import (
	"binary"
	"log"
)

import (
	"github.com/timtadh/fs2/fmap"
	"github.com/timtadh/fs2/mmlist"
)

func main() {
	file, err := fmap.CreateBlockFile("/tmp/file")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	list, err := mmlist.New(file)
	if err != nil {
		log.Fatal(err)
	}
	idx, err := list.Append([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	if d, err := list.Get(idx); err != nil {
		log.Fatal(err)
	} else if !bytes.Equal([]byte("hello"), d) {
		log.Fatal("bytes where not hello")
	}
	if err := list.Set(idx, "bye!"); err != nil {
		log.Fatal(err)
	}
	if d, err := list.Get(idx); err != nil {
		log.Fatal(err)
	} else if !bytes.Equal([]byte("bye!"), d) {
		log.Fatal("bytes where not bye!")
	}
	if d, err := list.Pop(); err != nil {
		log.Fatal(err)
	} else if !bytes.Equal([]byte("bye!"), d) {
		log.Fatal("bytes where not bye!")
	}
}
```

## `fs2-generic`

A command to generate type specialized wrappers around fs2 structures.

Since Go does not support generics and is not going to support generics anytime
soon, this program will produce a wrapper specialized to the supplied types. It
is essentially manually implementing type specialized generics in a very limited
form.  All fs2 structures operate on sequences of bytes, aka `[]byte`, because
they memory mapped and file backed structures. Therefore, the supplied types
must be serializable to be used as keys and values in an fs2 structure.

### How to install

Assuming you already have the code downloaded and in your GOPATH just run:

    $ go install github.com/timtadh/fs2/fs2-generic

#### How to generate a wrapper for the B+ Tree

    $ fs2-generic \
        --output=src/output/package/file.go \
        --package-name=package \
        bptree \
            --key-type=my/package/name/Type \
            --key-serializer=my/package/name/Func \
            --key-deserializer=my/package/name/Func \
            --value-type=my/package/name/Type \
            --value-serializer=my/package/name/Func \
            --value-deserializer=my/package/name/Func

#### How to generate a wrapper for the MMList

    $ fs2-generic \
        --output=src/output/package/file.go \
        --package-name=package \
        mmlist \
            --item-type=my/package/name/Type \
            --item-serializer=my/package/name/Func \
            --item-deserializer=my/package/name/Func

#### Variations

Supplying a pointer type:

    --key-type=*my/package/name/Type
    --value-type=*my/package/name/Type

Serializer Type Signature (let T be a type parameter)

    func(T) ([]byte)

Deserializer Type Signature (let T be a type parameter)

    func([]byte) T

Fixed sized types can have their sizes specified with

    --key-size=<# of bytes>
    --value-size=<# of bytes>

If the generated file is going into the same package that the types and
function are declared in one should drop the package specifiers

    $ fs2-generic \
        --output=src/output/package/file.go \
        --package-name=package \
        bptree \
            --key-type=KeyType \
            --key-serializer=SerializeKey \
            --key-deserializer=DeserializeKey \
            --value-type=ValueType \
            --value-serializer=SerializeValue \
            --value-deserializer=DeserializeValue

If `nil` is not a valid "empty" value for your type (for instance it is an
integer, a float, or a struct value) then your must supply a valid "empty"
value. Here is an example of a tree with int32 keys and float64 values:

    $ fs2-generic \
        --output=src/output/package/file.go \
        --package-name=package \
        bptree \
            --key-type=int32 \
            --key-size=4 \
            --key-empty=0 \
            --key-serializer=SerializeInt32 \
            --key-deserializer=DeserializeInt32 \
            --value-type=float64 \
            --value-size=8 \
            --value-empty=0.0 \
            --value-serializer=SerializeFloat64 \
            --value-deserializer=DeserializeFloat64

### Using with `go generate`

The fs2-generic command can be used on conjunction with `go generate`. To do so
simply create a `.go` file in the package where the generated code should live.
For example, let's pretend that we want to create a B+Tree with 3 dimension
integer points as keys and float64's as values. Lets create a package structure
for that (assuming you are in the root of your $GOPATH)

    mkdir ./src/edu/cwru/eecs/pointbptree/
    touch ./src/edu/cwru/eecs/pointbptree/types.go

`types.go` should then have the point defined + functions for serialization.
Below is the full example. Note the top line specifies how to generate the file
`./src/edu/cwru/eecs/pointbptree/wrapper.go`. To generate it run `go generate
edu/cwru/eecs/pointbptree`.

```go
//go:generate fs2-generic --output=wrapper.go --package-name=pointbptree bptree --key-type=*Point --key-size=12 --key-empty=nil --key-serializer=SerializePoint --key-deserializer=DeserializePoint --value-type=float64 --value-size=8 --value-empty=0.0 --value-serializer=SerializeFloat64 --value-deserializer=DeserializeFloat64
package pointbptree

import (
	"encoding/binary"
	"math"
)

type Point struct {
	X, Y, Z int32
}

func SerializePoint(p *Point) []byte {
	bytes := make([]byte, 4*3)
	binary.BigEndian.PutUint32(bytes[0:04], uint32(p.X))
	binary.BigEndian.PutUint32(bytes[4:08], uint32(p.Y))
	binary.BigEndian.PutUint32(bytes[8:12], uint32(p.Z))
	return bytes
}

func DeserializePoint(bytes []byte) *Point {
	return &Point{
		X: int32(binary.BigEndian.Uint32(bytes[0:04])),
		Y: int32(binary.BigEndian.Uint32(bytes[4:08])),
		Z: int32(binary.BigEndian.Uint32(bytes[8:12])),
	}
}

func SerializeFloat64(f float64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, math.Float64bits(f))
	return bytes
}

func DeserializeFloat64(bytes []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(bytes))
}
```


## FMap

[docs](https://godoc.org/github.com/timtadh/fs2/fmap)

FMap provides a block oriented interface for implementing memory mapped file
structures. It is block oriented because memory mapped structures **should** be
block aligned. By making the interface block oriented, the programmer is forced
to write the structures in a block oriented fashion. I use it with
[fs2/slice](https://godoc.org/github.com/timtadh/fs2/slice) which provides a
simple way to cast []byte to other types of pointers. You can accomplish a
similar thing with just using the `reflect` package but you might find
`fs2/slice` more convenient.

FMap provides an interface for creating both anonymous and file backed memory
maps. It supports resizing the memory maps dynamically via allocation and free
methods. Note, when an allocation occurs the underlying file and memory map
**may** resize using `mremap` with the flag `MREMAP_MAYMOVE`. So don't let
pointers escape your memory map! Keep everything as file offsets and be happy!

## Memory Mapped IO versus Read/Write

A key motivation of this work is to explore memory mapped IO versus a read/write
interface in the context of Go. I have two hypotheses:

1. The operating system is good at page management generally. While, we know
   more about how to manage the structure of B+Trees, VarChar stores, and Linear
   Hash tables than the OS there is no indication that from Go you can achieve
   better performance. Therefore, I hypothesize that leaving it to the OS will
   lead to a smaller working set and a faster data structure in general.

2. You can make Memory Mapping performant in Go. There are many challenges here.
   The biggest of which is that there are no dynamically size array TYPES in go.
   The size of the array is part of the type, you have to use slices. This
   creates complications when hooking up structures which contain slices to mmap
   allocated blocks of memory. I hypothesize that this repository can achieve
   good (enough) performance here.

In my past experience using the read/write interface I have encountered two
challenges:

1. When using the read/write interface one needs to block and cache management.
   In theory databases which bypass the OS cache management get better
   performance. In practice, there are challenges achieving this from a garbage
   collected language.

2. Buffer management is a related problem. In the past I have relied on Go's
   built in memory management scheme. This often become a bottle neck. To solve
   this problem, one must implement custom allocators and buffer management
   subsystems.

Memory mapped IO avoids both of these problems by delegating them to the
operating system. If the OS does a good job, then this system will perform well.
If it does a bad job it will perform poorly. The reason why systems such as
Oracle circumvent all OS level functions for page management is the designers
believe: a) they can do it better, and b) it provides consistent performance
across platforms.

Memory mapped IO in Go has several challenges.

1. You have to subvert type and memory safety.

2. There is no dynamically sized arrays. Therefore, everything has to use
   slices. This means that you can't just point a `struct` at a memory mapped
   block and expect it work if it has slices in it. Instead, some book keeping
   needs to be done to hook up the slices properly. This adds overhead.

The results so far:

1. It can be done

2. Integrating (partial) runtime checking for safety can be achieved through the
   use of the "do" interface.

3. The performance numbers look like they are as good or better than the
   Linear Hash table I implemented in my file-structures repository.

## Related Projects

1. [file-structures](https://github.com/timtadh/file-structures) - A collection
   of file-structures includes: B+Tree, BTree, Linear Hash Table, VarChar Store.
2. [data-structures](https://github.com/timtadh/data-structures) - A collection
   of in memory data structures. Includes a B+Tree.
3. [boltdb](https://github.com/boltdb/bolt) - a mmap'ed b+ tree based key/value
   store.
4. [goleveldb](https://github.com/syndtr/goleveldb) - another database written
   in go
5. [cznic/b](https://github.com/cznic/b) - an in memory b+ tree
6. [xiang90/bplustree](https://github.com/xiang90/bplustree) - an in memory b+
   tree
7. your project here.

