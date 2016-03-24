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

func parseType(imports map[string]bool, fqtn string) string {
	ptr := ""
	if strings.HasPrefix(fqtn, "*") {
		ptr = "*"
		fqtn = strings.TrimLeft(fqtn, "*")
	}
	parts := strings.Split(fqtn, "/")
	if len(parts) == 1 {
		return ptr + fqtn
	}
	typename := ptr + strings.Join(parts[len(parts)-2:], ".")
	imp := strings.Join(parts[:len(parts)-1], "/")
	imports[imp] = true
	return typename
}

func parseFunc(imports map[string]bool, fqfn string) string {
	parts := strings.Split(fqfn, "/")
	if len(parts) == 1 {
		return fqfn
	}
	funcname := strings.Join(parts[len(parts)-2:], ".")
	imp := strings.Join(parts[:len(parts)-1], "/")
	imports[imp] = true
	return funcname
}

func BpTree(fout io.Writer, packageName string, args []string) {
	_, optargs, err := getopt.GetOpt(
		args,
		"h",
		[]string{
			"help",
			"wat",
			"use-parameterized-serialization",
			"key-size=",
			"key-empty=",
			"key-type=",
			"key-serializer=",
			"key-deserializer=",
			"value-size=",
			"value-empty=",
			"value-type=",
			"value-serializer=",
			"value-deserializer=",
		},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		Usage(ErrorCodes["opts"])
	}

	parameters := false
	keySize := -1
	keyEmpty := "nil"
	keyType := ""
	keySerializer := ""
	keyDeserializer := ""
	valueSize := -1
	valueEmpty := "nil"
	valueType := ""
	valueSerializer := ""
	valueDeserializer := ""
	paths := make(map[string]bool)
	for _, oa := range optargs {
		switch oa.Opt() {
		case "-h", "--help":
			Usage(0)
		case "--use-parameterized-serialization":
			parameters = true
		case "--key-size":
			keySize = ParseInt(oa.Arg())
		case "--key-empty":
			keyEmpty = oa.Arg()
		case "--key-type":
			keyType = parseType(paths, oa.Arg())
		case "--key-serializer":
			keySerializer = parseFunc(paths, oa.Arg())
		case "--key-deserializer":
			keyDeserializer = parseFunc(paths, oa.Arg())
		case "--value-size":
			valueSize = ParseInt(oa.Arg())
		case "--value-empty":
			valueEmpty = oa.Arg()
		case "--value-type":
			valueType = parseType(paths, oa.Arg())
		case "--value-serializer":
			valueSerializer = parseFunc(paths, oa.Arg())
		case "--value-deserializer":
			valueDeserializer = parseFunc(paths, oa.Arg())
		default:
			fmt.Fprintf(os.Stderr, "Unknown flag '%v'\n", oa.Opt())
			Usage(ErrorCodes["opts"])
		}
	}

	if keyType == "" {
		fmt.Fprintln(os.Stderr, "Must supply a key-type")
		Usage(ErrorCodes["opts"])
	}

	if valueType == "" {
		fmt.Fprintln(os.Stderr, "Must supply a value-type")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && keySerializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a key-serializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && keySerializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && valueSerializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a value-serializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && valueSerializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && keyDeserializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a key-deserializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && keyDeserializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if !parameters && valueDeserializer == "" {
		fmt.Fprintln(os.Stderr, "Must supply a value-deserializer")
		Usage(ErrorCodes["opts"])
	} else if parameters && valueDeserializer != "" {
		fmt.Fprintln(os.Stderr, "Cannot supply serialization funcs and use serialization through constructor parameters")
		Usage(ErrorCodes["opts"])
	}

	if parameters {
		keySerializer = "b.serializeKey"
		valueSerializer = "b.serializeValue"
		keyDeserializer = "b.deserializeKey"
		valueDeserializer = "b.deserializeValue"
	}

	imports := make([]string, 0, len(paths))
	for k := range paths {
		imports = append(imports, k)
	}

	err = bptreeTmpl.Execute(fout, map[string]interface{}{
		"argv":             strings.Join(os.Args, " \\\n*     "),
		"packageName":      packageName,
		"imports":          imports,
		"useParameters":    parameters,
		"keySize":          keySize,
		"valueSize":        valueSize,
		"keyEmpty":         keyEmpty,
		"valueEmpty":       valueEmpty,
		"keyType":          keyType,
		"valueType":        valueType,
		"serializeKey":     keySerializer,
		"serializeValue":   valueSerializer,
		"deserializeKey":   keyDeserializer,
		"deserializeValue": valueDeserializer,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Template error \n%v\v", err)
		Usage(ErrorCodes["template"])
	}
}
