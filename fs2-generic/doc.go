/*
fs2-generic -- Generate type specialized wrappers around fs2 structures.

Since Go does not support generics and is not going to support generics anytime
soon, this program will produce a wrapper specialized to the supplied types. It
is essentially manually implementing type specialized generics in a very limited
form.  All fs2 structures operate on sequences of bytes, aka `[]byte`, because
they memory mapped and file backed structures. Therefore, the supplied types
must be serializable to be used as keys and values in an fs2 structure.

How to install

Assuming you already have the code downloaded and in your GOPATH just run:

    $ go install github.com/timtadh/fs2/fs2-generic

How to generate a wrapper for the B+ Tree

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

*/
package main
