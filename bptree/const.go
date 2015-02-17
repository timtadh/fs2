package bptree

type flag uint8

const (
	INTERNAL flag = 1 << iota
	LEAF
)

const (
	SMALL_VALUE flag = 1 << iota
	BIG_VALUE
)

