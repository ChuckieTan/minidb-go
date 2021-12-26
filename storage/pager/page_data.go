package pager

import (
	"bytes"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/util"
)

type PageData interface {
	Raw() []byte
}

type PageDataType uint8

const (
	META_DATA PageDataType = iota
	RECORE_DATA
	INDEX_DATA
)

func NewPageData(dataType PageDataType) PageData {
	panic("implement me")
}

func LoadPageData(r io.Reader, pageType PageType) PageData {
	panic("implement me")
}

type TableInfo struct {
	tableName     string
	tableId       uint16
	ColumnDefines []ast.ColumnDefine

	Indexs []IndexInfo

	firstPageNum util.UUID
	lastPageNum  util.UUID
}

type MetaData struct {
	checksum     uint32
	checksumCopy uint32

	version  string
	tables   []TableInfo
	freeList []pageFreeSpace
}

func (meta *MetaData) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 1024))
	util.Encode(buff, meta)
	return buff.Bytes()
}

type RecordData struct {
	record [][]ast.SQLExprValue
}

func (record *RecordData) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 1024))
	util.Encode(buff, record)
	return buff.Bytes()
}
