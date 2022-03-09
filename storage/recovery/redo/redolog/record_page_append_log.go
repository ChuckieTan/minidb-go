package redolog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"minidb-go/util"
)

type RecordPageAppendLog struct {
	lsn      int64
	tableId  uint16
	columnId uint16
	pageNum  util.UUID
	row      []byte
}

func NewRecordPageAppendLog(tableId uint16, columnId uint16, pageNum util.UUID,
	row []byte) *RecordPageAppendLog {

	return &RecordPageAppendLog{
		lsn:      -1,
		tableId:  tableId,
		columnId: columnId,
		pageNum:  pageNum,
		row:      row,
	}
}

func (log *RecordPageAppendLog) LSN() int64 {
	return log.lsn
}

func (log *RecordPageAppendLog) SetLSN(LSN int64) {
	log.lsn = LSN
}

func (log *RecordPageAppendLog) TableId() uint16 {
	return log.tableId
}

func (log *RecordPageAppendLog) ColumnId() uint16 {
	return log.columnId
}

func (log *RecordPageAppendLog) PageNum() util.UUID {
	return log.pageNum
}

func (log *RecordPageAppendLog) Row() []byte {
	return log.row
}

func (log *RecordPageAppendLog) Type() LogType {
	return RECORD_PAGE_APPEND
}

func (log *RecordPageAppendLog) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, log.Type())
	gob.NewDecoder(buf).Decode(log)
	return buf.Bytes()
}

func (log *RecordPageAppendLog) Decode(r io.Reader) {
	gob.NewDecoder(r).Decode(log)
}
