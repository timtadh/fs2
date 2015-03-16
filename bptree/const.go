package bptree

type Flag uint8

const BLOCKSIZE = 4096

const (
	iNTERNAL Flag = 1 << iota
	lEAF
	vARCHAR_CTRL
	vARCHAR_FREE
	vARCHAR_RUN
	VARCHAR_KEYS
	VARCHAR_VALS
)
