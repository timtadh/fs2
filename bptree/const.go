package bptree

type Flag uint8

const BLOCKSIZE = 4096

const (
	iNTERNAL Flag = 1 << iota
	lEAF
	VARCHAR_KEYS
	VARCHAR_VALS
)

