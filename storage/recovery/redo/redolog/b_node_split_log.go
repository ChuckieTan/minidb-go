package redolog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"minidb-go/util"
)

type BNodeSplitLog struct {
	lsn         int64
	tableId     uint16
	columnId    uint16
	pageNum     util.UUID
	nextPageNum util.UUID
}

func NewBNodeSplitLog(tableId uint16, columnId uint16, pageNum util.UUID,
	nextPageNum util.UUID) *BNodeSplitLog {

	return &BNodeSplitLog{
		lsn:         -1,
		tableId:     tableId,
		columnId:    columnId,
		pageNum:     pageNum,
		nextPageNum: nextPageNum,
	}
}

func (log *BNodeSplitLog) LSN() int64 {
	return log.lsn
}

func (log *BNodeSplitLog) SetLSN(LSN int64) {
	log.lsn = LSN
}

func (log *BNodeSplitLog) TableId() uint16 {
	return log.tableId
}

func (log *BNodeSplitLog) ColumnId() uint16 {
	return log.columnId
}

func (log *BNodeSplitLog) PageNum() util.UUID {
	return log.pageNum
}

func (log *BNodeSplitLog) NextPageNum() util.UUID {
	return log.nextPageNum
}

func (log *BNodeSplitLog) Type() LogType {
	return B_NODE_SPLIT
}

func (log *BNodeSplitLog) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, log.Type())
	gob.NewEncoder(buf).Encode(log)
	return buf.Bytes()
}

func (log *BNodeSplitLog) Decode(r io.Reader) {
	gob.NewDecoder(r).Decode(log)
}
