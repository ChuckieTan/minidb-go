package pager

import (
	"io"
	"minidb-go/parser/ast"
)

type ColumnDefine ast.ColumnDefine

type TableDefine struct {
	TableName        string
	RootPage         uint64
	FirstLeafPage    uint64
	LastLeafPage     uint64
	ColumnDefineList []ColumnDefine
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
