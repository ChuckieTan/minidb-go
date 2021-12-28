package pager

import (
	"bytes"
	"encoding/gob"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/util"
)

type PageData interface {
	gob.GobEncoder
	gob.GobDecoder
	// 返回 PageData 的大小，以字节为单位
	Size() int
	PageDataType() PageDataType
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

// 应该放入 TableManage 里面
// 索引信息
// type IndexInfo struct {
// 	ColumnId  uint16
// 	BPlusTree *BPlusTree
// }

type TableInfo struct {
	tableName     string
	tableId       uint16
	ColumnDefines []ast.ColumnDefine

	// Indexs map[uint16]*IndexInfo

	firstPageNum util.UUID
	lastPageNum  util.UUID
}

type MetaData struct {
	checksum     uint32
	checksumCopy uint32

	version string
	tables  []TableInfo
}

func NewMetaData() *MetaData {
	return &MetaData{}
}
func (m *MetaData) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *MetaData) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(m)
}

func (meta *MetaData) Size() int {
	raw, _ := meta.GobEncode()
	return len(raw)
}

func (meta *MetaData) PageDataType() PageDataType {
	return META_DATA
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
