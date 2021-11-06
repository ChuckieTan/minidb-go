package pager

import (
	"io"
	"minidb-go/parser/ast"
)

type TableDefine struct {
	TableName       string
	RootAddr        uint64
	FirstLeafAddr   uint64
	LastLeafAddr    uint64
	ColumnDeineList []ast.ColumnDefine
}

type MetaPage struct {
	Minidb          string
	TableDefineList []TableDefine
}

type IndexPage struct {
}

type DataPage struct {
	DataList [][]ast.SQLExprValue
}
type Pager struct {
	Writer io.Writer
}
