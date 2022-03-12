package ast

import (
	"encoding/binary"
	"fmt"
	"io"
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
	ValueType() SQLValueType
	Raw() []byte
	Encode(w io.Writer)
	Decode(r io.Reader)
	String() string
	DeepCopy() SQLExprValue
}

func (sqlInt *SQLInt) ValueType() SQLValueType {
	return SQL_INT
}
func (sqlFloat *SQLFloat) ValueType() SQLValueType {
	return SQL_FLOAT
}
func (sqlText *SQLText) ValueType() SQLValueType {
	return SQL_TEXT
}
func (sqlColumn *SQLColumn) ValueType() SQLValueType {
	return SQL_COLUMN
}

func (sqlInt *SQLInt) Raw() []byte {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(*sqlInt))
	return raw
}
func (sqlInt *SQLInt) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, SQL_INT)
	binary.Write(w, binary.BigEndian, sqlInt)
}
func (sqlInt *SQLInt) Decode(r io.Reader) {
	binary.Read(r, binary.BigEndian, sqlInt)
}

func (sqlFloat *SQLFloat) Raw() []byte {
	raw := make([]byte, 8)
	bits := math.Float64bits(float64(*sqlFloat))
	binary.BigEndian.PutUint64(raw, bits)
	return raw
}
func (sqlFloat *SQLFloat) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, SQL_FLOAT)
	binary.Write(w, binary.BigEndian, sqlFloat)
}
func (sqlFloat *SQLFloat) Decode(r io.Reader) {
	binary.Read(r, binary.BigEndian, sqlFloat)
}

func (sqlText *SQLText) Raw() []byte {
	raw := []byte(*sqlText)[:util.BPLUSTREE_KEY_LEN+1]
	return raw
}
func (sqlText *SQLText) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, SQL_TEXT)
	binary.Write(w, binary.BigEndian, uint16(len(*sqlText)))
	w.Write([]byte(*sqlText))
}
func (sqlText *SQLText) Decode(r io.Reader) {
	var size uint16
	binary.Read(r, binary.BigEndian, &size)
	buf := make([]byte, size)
	r.Read(buf)
	*sqlText = SQLText(buf)
}

func (sqlColumn *SQLColumn) Raw() []byte {
	raw := []byte(*sqlColumn)[:util.BPLUSTREE_KEY_LEN+1]
	return raw
}
func (sqlColumn *SQLColumn) Encode(w io.Writer) {
	binary.Write(w, binary.BigEndian, SQL_COLUMN)
	binary.Write(w, binary.BigEndian, uint16(len(*sqlColumn)))
	w.Write([]byte(*sqlColumn))
}
func (sqlColumn *SQLColumn) Decode(r io.Reader) {
	var size uint16
	binary.Read(r, binary.BigEndian, &size)
	buf := make([]byte, size)
	r.Read(buf)
	*sqlColumn = SQLColumn(buf)
}

func (sqlInt *SQLInt) String() string {
	return fmt.Sprint(*sqlInt)
}

func (sqlFloat *SQLFloat) String() string {
	return fmt.Sprintf("%f", *sqlFloat)
}

func (sqlText *SQLText) String() string {
	return fmt.Sprint(*sqlText)
}

func (sqlColumn *SQLColumn) String() string {
	return fmt.Sprint(*sqlColumn)
}

func (sqlInt *SQLInt) DeepCopy() SQLExprValue {
	val := SQLInt(*sqlInt)
	return &val
}

func (sqlFloat *SQLFloat) DeepCopy() SQLExprValue {
	val := SQLFloat(*sqlFloat)
	return &val
}

func (sqlText *SQLText) DeepCopy() SQLExprValue {
	val := SQLText(*sqlText)
	return &val
}

func (sqlColumn *SQLColumn) DeepCopy() SQLExprValue {
	val := SQLColumn(*sqlColumn)
	return &val
}

func SQLValueEqual(left, right SQLExprValue) bool {
	if left.ValueType() != right.ValueType() {
		return false
	}
	switch left.ValueType() {
	case SQL_INT:
		return *left.(*SQLInt) == *right.(*SQLInt)
	case SQL_FLOAT:
		return *left.(*SQLFloat) == *right.(*SQLFloat)
	case SQL_TEXT:
		return *left.(*SQLText) == *right.(*SQLText)
	case SQL_COLUMN:
		return *left.(*SQLColumn) == *right.(*SQLColumn)
	default:
		return false
	}
}

func decodeExprValue(r io.Reader) (SQLExprValue, error) {
	var valueType SQLValueType
	if err := binary.Read(r, binary.BigEndian, &valueType); err != nil {
		return nil, err
	}
	switch valueType {
	case SQL_INT:
		var sqlInt SQLInt
		sqlInt.Decode(r)
		return &sqlInt, nil
	case SQL_FLOAT:
		var sqlFloat SQLFloat
		sqlFloat.Decode(r)
		return &sqlFloat, nil
	case SQL_TEXT:
		var sqlText SQLText
		sqlText.Decode(r)
		return &sqlText, nil
	case SQL_COLUMN:
		var sqlColumn SQLColumn
		sqlColumn.Decode(r)
		return &sqlColumn, nil
	default:
		return nil, fmt.Errorf("unknown value type: %d", valueType)
	}
}

type SQLExpr struct {
	Left  SQLExprValue
	Op    token.TokenType
	Right SQLExprValue
}

func (expr SQLExpr) IsEqual() bool {
	return expr.Op == token.TT_ASSIGN || expr.Op == token.TT_EQUAL
}

func (expr SQLExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", expr.Left, expr.Op, expr.Right)
}
