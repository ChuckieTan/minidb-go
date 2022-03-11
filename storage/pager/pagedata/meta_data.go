package pagedata

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"minidb-go/parser/ast"
	"minidb-go/util"

	log "github.com/sirupsen/logrus"
)

type TableInfo struct {
	TableName string
	TableId   uint16

	ColumnDefines []*ast.ColumnDefine

	FirstPageNum util.UUID
	LastPageNum  util.UUID
}

func (ti *TableInfo) PrimaryKey() string {
	return ti.ColumnDefines[0].Name
}

func (ti *TableInfo) ColumnNames() []string {
	columnNames := make([]string, 0)
	for _, columnDefine := range ti.ColumnDefines {
		columnNames = append(columnNames, columnDefine.Name)
	}
	return columnNames
}

func (ti *TableInfo) GetColumnDefine(columnName string) *ast.ColumnDefine {
	for _, columnDefine := range ti.ColumnDefines {
		if columnDefine.Name == columnName {
			return columnDefine
		}
	}
	return nil
}

func (ti *TableInfo) SetLastPageNum(uuid util.UUID) {
	ti.LastPageNum = uuid
}

type MetaData struct {
	Version string
	Tables  map[string]*TableInfo
}

func (meta *MetaData) GetTableInfo(tableName string) *TableInfo {
	for _, table := range meta.Tables {
		if table.TableName == tableName {
			return table
		}
	}
	return nil
}

func (meta *MetaData) AddTable(tableInfo *TableInfo) error {
	if meta.GetTableInfo(tableInfo.TableName) != nil {
		return errors.New("table already exists")
	}
	meta.Tables[tableInfo.TableName] = tableInfo
	return nil
}

func NewMetaData() *MetaData {
	return &MetaData{
		Version: util.VERSION,
		Tables:  make(map[string]*TableInfo, 0),
	}
}

func (m *MetaData) Encode() []byte {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(m)
	if err != nil {
		log.Error("meta data encode failed: ", err)
		return nil
	}
	return buf.Bytes()
}

func (m *MetaData) Decode(r io.Reader) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(m)
}

func (meta *MetaData) Size() int {
	raw := meta.Encode()
	return len(raw)
}
