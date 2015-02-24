/*
A Memory Mapped B+ Tree

This is a low level file structure. It is a memory mapped B+ Tree. It
is not thread safe nor is it safe to access the backing file from
multiple processes at once.

Features:

1. Fixed size key. Set at B+ Tree creation. Key resizes mean the tree
needs to be recreated.

2. Variable length values. They can very from 0 bytes to 2^32 - 1 bytes.

3. Duplicate key support. Duplicates are kept out of the index and
only occur in the leaves.

Creating a new *BpTree

	bf, err := fmap.CreateBlockFile("/path/to/file")
	if err != nil {
		log.Fatal(err)
	}
	defer bf.Close()
	bpt, err := New(bf, 8)
	if err != nil {
		log.Fatal(err)
	}
	// do stuff with bpt

Opening a *BpTree

	bf, err := fmap.OpenBlockFile("/path/to/file")
	if err != nil {
		log.Fatal(err)
	}
	defer bf.Close()
	bpt, err := Open(bf)
	if err != nil {
		log.Fatal(err)
	}
	// do stuff with bpt

Add a key/value pair. Note, since this is low level you have to
serialize your keys and values. The length of the []byte representing
the key must exactly match the key size of the B+ Tree. You can find out
what that was set to by called `bpt.KeySize()`

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

As you can see it can be a little verbose to serialize and deserialize
your keys and values. So be sure to wrap that up in utility functions or
even to wrap the interface of the *BpTree so that client code does not
have to think about it.

Since a B+Tree is a "multi-map" meaning there may be more than one value
per key. There is no "Get" method. To retrieve the values associated
with a key use the `Find` method.

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

That interface is easy to misuse if you do not check the error values as
show in the example above. An easier interface is provided for all of
the iterators (Range, Find, Keys, Values, Iterate) called the Do iterface.

	err = bpt.DoFind(kBytes,
		func(key, value []byte) error {
			// do stuff with the keys and values
			return nil
	})
	if err != nil {
		log.Fatal(err)
	}

It is recommended that you always use the Do* interfaces. The other is
provided if the cost of extra method calls is too high.

Removal is also slightly more complicated due to the duplicate keys.
This example will remove all key/value pairs associated with the given
key:

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

That wraps up the basic usage. If you want to ensure that the bytes you
have written are in fact on disk you have 2 options

1. call bf.Sync() - Note this uses the async mmap interface under the
hood. The bytes are not guarateed to hit the disk after this returns but
they will go there soon.

2. call bf.Close()

*/
package bptree
