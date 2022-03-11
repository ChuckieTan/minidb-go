package redolog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"minidb-go/util"
)

type BNodeInsertKVLog struct {
	lsn      int64
	tableId  uint16
	columnId uint16
	pageNum  util.UUID
	key      []byte
	value    []byte
}

func NewBNodeInsertKVLog(tableId uint16, columnId uint16, pageNum util.UUID,
	key []byte, value []byte) *BNodeInsertKVLog {

	return &BNodeInsertKVLog{
		lsn:      -1,
		tableId:  tableId,
		columnId: columnId,
		pageNum:  pageNum,
		key:      key,
		value:    value,
	}
}

func (log *BNodeInsertKVLog) LSN() int64 {
	return log.lsn
}

func (log *BNodeInsertKVLog) SetLSN(LSN int64) {
	log.lsn = LSN
}

func (log *BNodeInsertKVLog) TableId() uint16 {
	return log.tableId
}

func (log *BNodeInsertKVLog) ColumnId() uint16 {
	return log.columnId
}

func (log *BNodeInsertKVLog) PageNum() util.UUID {
	return log.pageNum
}

func (log *BNodeInsertKVLog) Key() []byte {
	return log.key
}

func (log *BNodeInsertKVLog) Value() []byte {
	return log.value
}

func (log *BNodeInsertKVLog) Type() LogType {
	return B_NODE_INSERT_KV
}

// 编码 B_NODE_INSERT_KV 日志
func (log *BNodeInsertKVLog) Bytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, log.Type())
	gob.NewEncoder(buf).Encode(log)
	return buf.Bytes()
}

func (log *BNodeInsertKVLog) Decode(r io.Reader) {
	gob.NewDecoder(r).Decode(log)
}
