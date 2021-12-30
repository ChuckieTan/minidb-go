package pager

import (
	"bytes"
	"encoding/gob"
	"io"
	"minidb-go/parser/ast"
)

type PageData interface {
	gob.GobEncoder
	gob.GobDecoder
	// 返回 PageData 的大小，以字节为单位
	Size() int
}

type PageDataType uint8

const (
	META_DATA PageDataType = iota
	RECORE_DATA
	INDEX_DATA
)

func LoadPageData(r io.Reader, pageType PageType) PageData {
	panic("implement me")
}

type RecordData struct {
	record []ast.DataEntry
}

func NewRecordData() *RecordData {
	return &RecordData{}
}

func (r *RecordData) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (r *RecordData) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(r)
}

func (record *RecordData) Record() []ast.DataEntry {
	return record.record
}

func (record *RecordData) Size() int {
	raw, _ := record.GobEncode()
	return len(raw)
}

func (record *RecordData) PageDataType() PageDataType {
	return RECORE_DATA
}
