# fs2 - File Structures 2

by Tim Henderson (tadh@case.edu)

Licensed under the GNU GPL version 3 or at your option any later version. If you
need another licensing option please contact me directly.

### What is this?

A [B+ Tree](#b-tree) implementation and a platform for implementing memory
mapped high performance file structures in Go.

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

This is a disk backed low level "key-value store". The closest thing similar to
what it offers is [Bolt DB](https://github.com/boltdb/bolt). My [blog
post](http://hackthology.com/lessons-learned-while-implementing-a-btree.html) is
a great place to start to learn more about the ubiquitous B+ Tree.

### Features

1. Fixed size key. Set at B+ Tree creation. Key resizes mean the tree needs to
   be recreated.

2. Variable length values. They can very from 0 bytes to 2^32 - 1 bytes.

3. Duplicate key support. Duplicates are kept out of the index and only occur in
   the leaves.

4. Data is only written to disk when you tell it (or when need due to OS level
   page management).

5. Simple (but low level) interface.

### Limitations

1. Not thread safe and therefore no transactions which you only need with
   multiple threads.

2. Maximum value size is 2^32 - 1

3. Maximum key size is ~1350 bytes.

4. This is not a database. You could make it into a database or build a database
   on top of it.

### Quick Start

Importing

	import (
		"github.com/timtadh/fs2/bptree"
		"github.com/timtadh/fs2/fmap"
	)

Creating a new B+ Tree

	bf, err := fmap.CreateBlockFile("/path/to/file")
	if err != nil {
		log.Fatal(err)
	}
	defer bf.Close()
	bpt, err := bptree.New(bf, 8)
	if err != nil {
		log.Fatal(err)
	}
	// do stuff with bpt

Opening a B+ Tree

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

Add a key/value pair. Note, since this is low level you have to serialize your
keys and values. The length of the []byte representing the key must exactly
match the key size of the B+ Tree. You can find out what that was set to by
called `bpt.KeySize()`

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

As you can see it can be a little verbose to serialize and de-serialize your
keys and values. So be sure to wrap that up in utility functions or even to wrap
the interface of the B+ Tree so that client code does not have to think about
it.

Since a B+Tree is a "multi-map" meaning there may be more than one value per
key. There is no "Get" method. To retrieve the values associated with a key use
the `Find` method.

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

That interface is easy to misuse if you do not check the error values as show in
the example above. An easier interface is provided for all of the iterators
(Range, Find, Keys, Values, Iterate) called the Do interface.

	err = bpt.DoFind(kBytes, func(key, value []byte) error {
		// do stuff with the keys and values
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

It is recommended that you always use the Do\* interfaces. The other is provided
if the cost of extra method calls is too high.

Removal is also slightly more complicated due to the duplicate keys.  This
example will remove all key/value pairs associated with the given key:

	err = bpt.Remove(kBytes, func(value []byte) bool {
		return true
	})
	if err != nil {
		log.Fatal(err)
	}

to remove just the one I added earlier do:

	err = bpt.Remove(kBytes, func(v []byte) bool {
		return bytes.Equal(v, []byte(value))
	})
	if err != nil {
		log.Fatal(err)
	}

That wraps up the basic usage. If you want to ensure that the bytes you have
written are in fact on disk you have 2 options

1. call bf.Sync() - Note this uses the async mmap interface under the hood. The
   bytes are not guaranteed to hit the disk after this returns but they will go
   there soon.

2. call bf.Close()

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

