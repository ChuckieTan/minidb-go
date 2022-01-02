package pagedata

import (
	"bytes"
	"encoding/gob"
	"io"
	"minidb-go/parser/ast"
)

type RecordData struct {
	rows []ast.Row
}

func NewRecordData() *RecordData {
	return &RecordData{}
}

func (r *RecordData) Encode() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(r)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (record *RecordData) Decode(r io.Reader) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(record)
}

func (record *RecordData) Rows() []ast.Row {
	return record.rows
}

func (record *RecordData) Size() int {
	raw := record.Encode()
	return len(raw)
}

func (record *RecordData) Append(rows ast.Row) {
	record.rows = append(record.rows, rows)
}

func (record *RecordData) PageDataType() PageDataType {
	return RECORE_DATA
}
