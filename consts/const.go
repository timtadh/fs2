package consts

type Flag uint8

const BLOCKSIZE = 4096

const (
	INTERNAL Flag = 1 << iota
	LEAF
	VARCHAR_CTRL
	VARCHAR_FREE
	VARCHAR_RUN
	VARCHAR_KEYS
	VARCHAR_VALS
)
