package index

import "minidb-go/parser/ast"

type IndexType uint8

const (
	PRIMARY_INDEX IndexType = iota // 主键索引
	SIMPLE_INDEX                   // 简单索引
)

// 索引信息
type IndexInfo struct {
	ColumnId  uint16
	Index     Index
	IndexType IndexType

	KeyType   ast.SQLValueType
	ValueType ast.SQLValueType
}
