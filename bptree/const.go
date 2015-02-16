package bptree

type flag uint8

const (
	INTERNAL flag = 1 << iota
	LEAF
	VALUE
)

