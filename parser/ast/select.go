package ast

import (
	"bytes"
	"encoding/gob"
)

type SelectStatement struct {
	ResultList  []string
	TableSource string
	Where       WhereStatement
}

func (statement SelectStatement) StatementType() string {
	return "Select"
}

type Row []SQLExprValue

func (row Row) Len() uint16 {
	buff := bytes.NewBuffer(make([]byte, 0))
	encoder := gob.NewEncoder(buff)
	encoder.Encode(row)
	return uint16(buff.Len())
}
