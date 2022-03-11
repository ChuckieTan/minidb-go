package ast

import "minidb-go/storage/index"

type ColumnType uint8

const (
	CT_INT ColumnType = iota
	CT_FLOAT
	CT_TEXT
)

type ColumnDefine struct {
	Type     ColumnType
	Name     string
	ColumnId uint16

	Index index.Index
}

func (columnDeine *ColumnDefine) SetColumnType(str string) {
	var columnType ColumnType = CT_INT
	switch str {
	case "int":
		columnType = CT_INT
	case "float":
		columnType = CT_FLOAT
	case "text":
		columnType = CT_TEXT
	}
	columnDeine.Type = columnType
}

type CreateTableStatement struct {
	TableName     string
	ColumnDefines []ColumnDefine
}

func (statement CreateTableStatement) StatementType() string {
	return "Create table"
}
