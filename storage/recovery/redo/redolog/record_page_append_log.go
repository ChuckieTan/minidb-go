package redolog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/util"
)

type RecordPageAppendLog struct {
	lsn     int64
	pageNum util.UUID
	row     *ast.Row
}

func NewRecordPageAppendLog(pageNum util.UUID, row *ast.Row) *RecordPageAppendLog {
	return &RecordPageAppendLog{
		pageNum: pageNum,
		row:     row,
	}
}

func (log *RecordPageAppendLog) LSN() int64 {
	return log.lsn
}

func (log *RecordPageAppendLog) SetLSN(LSN int64) {
	log.lsn = LSN
}

func (log *RecordPageAppendLog) PageNum() util.UUID {
	return log.pageNum
}

func (log *RecordPageAppendLog) Row() *ast.Row {
	return log.row
}

func (log *RecordPageAppendLog) Type() LogType {
	return RECORD_PAGE_APPEND
}

func (log *RecordPageAppendLog) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, log.Type())
	gob.NewEncoder(buf).Encode(log)
	return buf.Bytes()
}

func (log *RecordPageAppendLog) Decode(r io.Reader) {
	gob.NewDecoder(r).Decode(log)
}
