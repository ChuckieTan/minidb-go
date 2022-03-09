package redolog

import (
	"encoding/binary"
	"errors"
	"io"
)

type LogType uint8

const (
	B_NODE_INSERT_KV LogType = iota
	B_NODE_SPLIT

	RECORD_PAGE_APPEND
)

var ErrUnknownLogType = errors.New("unknown log type")

type Log interface {
	LSN() int64
	Type() LogType
	Bytes() []byte
	Decode(r io.Reader)
}

func ReadLog(r io.Reader) (Log, error) {
	var logType LogType
	err := binary.Read(r, binary.BigEndian, &logType)
	if err != nil {
		return nil, err
	}
	var log Log
	switch logType {
	case B_NODE_INSERT_KV:
		log = &BNodeInsertKVLog{}
	case B_NODE_SPLIT:
		log = &BNodeSplitLog{}
	case RECORD_PAGE_APPEND:
		log = &RecordPageAppendLog{}
	default:
		return nil, ErrUnknownLogType
	}
	log.Decode(r)
	return log, nil
}
