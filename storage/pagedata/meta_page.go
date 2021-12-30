package pagedata

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"minidb-go/parser/ast"
	"minidb-go/storage/index"
	"minidb-go/util"
)

type TableInfo struct {
	tableName     string
	tableId       uint16
	columnDefines []ast.ColumnDefine

	indexs map[string]index.IndexInfo

	firstPageNum util.UUID
	lastPageNum  util.UUID
}

func (ti *TableInfo) Indexs() map[string]index.IndexInfo {
	return ti.indexs
}

func (ti *TableInfo) ColumnDefines() []ast.ColumnDefine {
	return ti.columnDefines
}

func (ti *TableInfo) TableName() string {
	return ti.tableName
}

func (ti *TableInfo) TableId() uint16 {
	return ti.tableId
}

func (ti *TableInfo) FirstPageNum() util.UUID {
	return ti.firstPageNum
}

func (ti *TableInfo) LastPageNum() util.UUID {
	return ti.lastPageNum
}

func (ti *TableInfo) SetLastPageNum(uuid util.UUID) {
	ti.lastPageNum = uuid
}

type MetaData struct {
	checksum     uint32
	checksumCopy uint32

	version string
	tables  []TableInfo
}

func (md *MetaData) Valid() bool {
	return md.checksum == md.checksumCopy
}

func (meta *MetaData) Version() string {
	return meta.version
}

func (meta *MetaData) GetTableInfo(tableName string) *TableInfo {
	for _, table := range meta.tables {
		if table.tableName == tableName {
			return &table
		}
	}
	return nil
}

func (meta *MetaData) GetTableInfoByTableId(tableId uint16) *TableInfo {
	for _, table := range meta.tables {
		if table.tableId == tableId {
			return &table
		}
	}
	return nil
}

func NewMetaData() *MetaData {
	return &MetaData{
		checksum:     rand.Uint32(),
		checksumCopy: 0,
		version:      util.VERSION,
		tables:       make([]TableInfo, 0),
	}
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
