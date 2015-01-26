package bptree

type flag uint8

const INTERNAL flag = 0
const (
	LEAF flag = 1 << iota
	BIG_LEAF
	BIG_CHAIN
)

