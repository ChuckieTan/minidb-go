package ast

type ColumnType int

const (
	CT_INT = iota
	CT_FLOAT
	CT_TEXT
)

type ColumnDefine struct {
	Type ColumnType
	Name string
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
	TableName       string
	ColumnDeineList []ColumnDefine
}
