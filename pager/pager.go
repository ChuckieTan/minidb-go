package pager

import (
	"io"
	"minidb-go/parser/ast"
)

type TableDefine struct {
	TableName       string
	RootAddr        uint32
	FirstLeafAddr   uint32
	LastLeafAddr    uint32
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
