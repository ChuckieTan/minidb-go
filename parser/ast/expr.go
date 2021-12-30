package ast

import (
	"encoding/binary"
	"math"
	"minidb-go/parser/token"
	"minidb-go/util"
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
	Raw() []byte
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

func (sqlInt SQLInt) Raw() []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(sqlInt))
	return raw
}
func (sqlFloat SQLFloat) Raw() []byte {
	raw := make([]byte, 8)
	bits := math.Float64bits(float64(sqlFloat))
	binary.BigEndian.PutUint64(raw, bits)
	return raw
}
func (sqlText SQLText) Raw() []byte {
	raw := []byte(sqlText)[:util.BPLUSTREE_KEY_LEN]
	return raw
}
func (sqlColumn SQLColumn) Raw() []byte {
	raw := []byte(sqlColumn)[:util.BPLUSTREE_KEY_LEN]
	return raw
}

type SQLExpr struct {
	Left  SQLExprValue
	Op    token.TokenType
	Right SQLExprValue
}

func (expr SQLExpr) IsEqual() bool {
	return expr.Op == token.TT_EQUAL
}
