package redo

import (
	"io"
	"minidb-go/util"
)

type logType uint8

const (
	B_LEAF_NODE_INSERT_KV logType = iota
	B_NON_LEAF_NODE_INSERT_KV
	B_LEAF_NODE_SPLIT
	B_NON_LEAF_NODE_SPLIT

	RECORD_PAGE_APPEND
)

type Log struct {
	LSN     util.UUID
	logType logType
}

func (log *Log) Bytes() []byte {
	return nil
}

func (log *Log) Decode(r io.Reader) {

}
