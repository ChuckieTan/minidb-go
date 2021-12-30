package ast

import (
	"minidb-go/parser/token"
)

type SQLValueType uint8

const (
	SQL_INT SQLValueType = iota
	SQL_FLOAT
	SQL_TEXT
	SQL_COLUMN
)

type SQLInt int64
type SQLFloat float64
type SQLText string
type SQLColumn string

type SQLExprValue interface {
	isExpr() bool
	ValueType() SQLValueType
}

func (sqlInt SQLInt) isExpr() bool {
	return true
}
func (sqlFloat SQLFloat) isExpr() bool {
	return true
}
func (sqlText SQLText) isExpr() bool {
	return true
}
func (sqlColumn SQLColumn) isExpr() bool {
	return true
}

func (sqlInt SQLInt) ValueType() SQLValueType {
	return SQL_INT
}
func (sqlFloat SQLFloat) ValueType() SQLValueType {
	return SQL_FLOAT
}
func (sqlText SQLText) ValueType() SQLValueType {
	return SQL_TEXT
}
func (sqlColumn SQLColumn) ValueType() SQLValueType {
	return SQL_COLUMN
}

type SQLExpr struct {
	Left  SQLExprValue
	Op    token.TokenType
	Right SQLExprValue
}

func (expr SQLExpr) IsEqual() bool {
	return expr.Op == token.TT_EQUAL
}
