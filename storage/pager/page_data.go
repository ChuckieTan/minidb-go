package pager

import (
	"bytes"
	"encoding/gob"
	"io"
	"minidb-go/parser/ast"
)

type PageData interface {
	Encode() []byte
	Decode(r io.Reader) error
	// 返回 PageData 的大小，以字节为单位
	Size() int
}

type PageDataType uint8

const (
	META_DATA PageDataType = iota
	RECORE_DATA
	INDEX_DATA
)

type RecordData struct {
	records []ast.Row
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

func (record *RecordData) Record() []ast.Row {
	return record.records
}

func (record *RecordData) Size() int {
	raw := record.Encode()
	return len(raw)
}

func (record *RecordData) PageDataType() PageDataType {
	return RECORE_DATA
}
