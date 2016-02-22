package main

/* Tim Henderson (tadh@case.edu)
*
* Copyright (c) 2015, Tim Henderson, Case Western Reserve University
* Cleveland, Ohio 44106. All Rights Reserved.
*
* This library is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 3 of the License, or (at
* your option) any later version.
*
* This library is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this library; if not, write to the Free Software
* Foundation, Inc.,
*   51 Franklin Street, Fifth Floor,
*   Boston, MA  02110-1301
*   USA
 */

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

import (
	"github.com/timtadh/getopt"
)


var ErrorCodes map[string]int = map[string]int{
	"usage":   0,
	"version": 2,
	"opts":    3,
	"badint":  5,
	"baddir":  6,
	"badfile": 7,
	"template": 7,
}

var UsageMessage string = "fs2-generic --help"
var ExtendedMessage string = `
fs2-generic -- generate a wrapper around a fs2 structure specialized
               to a specific data type

There is a subcommand for each supported data type.

Global Options
  -h, --help                view this message
  --types                   query what types are supported
  -o, --output=<path>       where to put the output (default stdout)

bptree

  $ fs2-generic bptree --key-type=my/package/name/Type \
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

  Options
    -h, --help                         view this message
    --use-parameterized-serialization  supply the serialization functions
                                       to the BpTree constructors
    --key-size=<int>                   default: variably sized
    --key-empty=<string>               empty value, default:nil
    --key-type=<type>
    --key-serializer=<func>
    --key-deserializer=<func>
    --value-size=<int>                 default: variably sized
    --value-empty=<string>             empty value, default:nil
    --value-type=<type>
    --value-serializer=<func>
    --value-deserializer=<func>

mmlist

  $ fs2-generic mmlist --item-type=my/package/name/Type \
                       --item-serializer=my/package/name/Func \
                       --item-deserializer=my/package/name/Func

  Supplying a pointer type:

      --item-type=*my/package/name/Type

  Serializer Type Signature (let T be a type parameter)

      func(T) ([]byte)

  Deserializer Type Signature (let T be a type parameter)

      func([]byte) T

  Options
    -h, --help                         view this message
    --use-parameterized-serialization  supply the serialization functions
                                       to the BpTree constructors
    --item-empty=<string>              empty value, default:nil
    --item-type=<type>
    --item-serializer=<func>
    --item-deserializer=<func>
`

func Usage(code int) {
	fmt.Fprintln(os.Stderr, UsageMessage)
	if code == 0 {
		fmt.Fprintln(os.Stdout, ExtendedMessage)
		code = ErrorCodes["usage"]
	} else {
		fmt.Fprintln(os.Stderr, "Try -h or --help for help")
	}
	os.Exit(code)
}

func ParseInt(str string) int {
	i, err := strconv.Atoi(str)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing '%v' expected an int\n", str)
		Usage(ErrorCodes["badint"])
	}
	return i
}

func AssertFile(fname string) string {
	fname = path.Clean(fname)
	fi, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		return fname
	} else if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		Usage(ErrorCodes["badfile"])
	} else if fi.IsDir() {
		fmt.Fprintf(os.Stderr, "Passed in file was a directory, %s", fname)
		Usage(ErrorCodes["badfile"])
	}
	return fname
}

func main() {
	args, optargs, err := getopt.GetOpt(
		os.Args[1:],
		"ho:",
		[]string{
			"help", "output=", "package-name=", "types",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	types := map[string]func(io.Writer,string,[]string) {
		"bptree": BpTree,
		"mmlist": MMList,
	}

	outputPath := ""
	packageName := ""
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "-o", "--output":
			outputPath = AssertFile(oa.Arg())
		case "--package-name":
			packageName = oa.Arg()
		case "--types":
			fmt.Fprintf(os.Stderr, "Types\n")
			for typename := range types {
				fmt.Fprintf(os.Stderr, "  %v\n", typename)
			}
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "Unknown flag '%v'\n", oa.Opt())
			Usage(ErrorCodes["opts"])
		}
	}

	if len(args) <= 0 {
		fmt.Fprintln(os.Stderr, "Must supply a type name, try --help")
		Usage(ErrorCodes["opts"])
	}

	typefunc, has := types[args[0]]
	if !has {
		fmt.Fprintf(os.Stderr, "Type '%v' not supported. Try --types to see support types.\n", args[0])
		Usage(ErrorCodes["opts"])
	}

	if packageName == "" {
		fmt.Fprintln(os.Stderr, "Must supply a package name, try --help")
		Usage(ErrorCodes["opts"])
	}

	var fout io.WriteCloser
	if outputPath == "" {
		fout = os.Stdout
	} else {
		fout, err = os.Create(outputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			Usage(ErrorCodes["opts"])
		}
	}
	defer fout.Close()

	typefunc(fout, packageName, args[1:])
}

