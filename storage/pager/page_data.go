package pager

import (
	"bytes"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/util"
)

type PageData interface {
	Raw() []byte
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

	// Indexs []IndexInfo

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

func (meta *MetaData) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 1024))
	util.Encode(buff, meta)
	return buff.Bytes()
}

func (meta *MetaData) Size() int {
	return len(meta.Raw())
}

func (meta *MetaData) PageDataType() PageDataType {
	return META_DATA
}

type DataEntry struct {
	Key  ast.SQLExprValue
	Data []ast.SQLExprValue
}

type RecordData struct {
	record []DataEntry
}

func NewRecordData() *RecordData {
	return &RecordData{}
}

func (record *RecordData) Record() []DataEntry {
	return record.record
}

func (record *RecordData) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 0))
	util.Encode(buff, record)
	return buff.Bytes()
}

func (record *RecordData) Size() int {
	return len(record.Raw())
}

func (record *RecordData) PageDataType() PageDataType {
	return RECORE_DATA
}
