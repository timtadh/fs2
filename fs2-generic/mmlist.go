package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

import (
	"github.com/timtadh/getopt"
)

func MMList(fout io.Writer, packageName string, args []string) {
	_, optargs, err := getopt.GetOpt(
		args,
		"h",
		[]string{
			"help",
			"use-parameterized-serialization",
			"item-empty=",
			"item-type=",
			"item-serializer=",
			"item-deserializer=",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	parameters := false
	itemEmpty := "nil"
	itemType := ""
	itemSerializer := ""
	itemDeserializer := ""
	paths := make(map[string]bool)
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "--use-parameterized-serialization":
			parameters = true
		case "--item-empty":
			itemEmpty = oa.Arg()
		case "--item-type":
			itemType = parseType(paths, oa.Arg())
		case "--item-serializer":
			itemSerializer = parseFunc(paths, oa.Arg())
		case "--item-deserializer":
			itemDeserializer = parseFunc(paths, oa.Arg())
		default:
			fmt.Fprintf(os.Stderr, "Unknown flag '%v'\n", oa.Opt())
			Usage(ErrorCodes["opts"])
		}
	}

	if itemType == "" {
		fmt.Fprintln(os.Stderr, "Must supply a item-type")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && itemSerializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a item-serializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && itemSerializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && itemDeserializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a item-deserializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && itemDeserializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if parameters {
		itemSerializer = "m.serializeItem"
		itemDeserializer = "m.deserializeItem"
	}

	imports := make([]string, 0, len(paths))
	for k := range paths {
		imports = append(imports, k)
	}

	err = mmlistTmpl.Execute(fout, map[string]interface{} {
		"argv": strings.Join(os.Args, " \\\n*     "),
		"packageName": packageName,
		"imports": imports,
		"useParameters": parameters,
		"itemEmpty": itemEmpty,
		"itemType": itemType,
		"serializeItem": itemSerializer,
		"deserializeItem": itemDeserializer,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Template error \n%v\v", err)
		Usage(ErrorCodes["template"])
	}
}

