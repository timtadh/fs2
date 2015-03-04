package bptree

type flag uint8

const BLOCKSIZE = 4096

const (
	iNTERNAL flag = 1 << iota
	lEAF
)

const (
	sMALL_VALUE flag = 1 << iota
	bIG_VALUE
)
