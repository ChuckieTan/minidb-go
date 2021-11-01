package ast

type InsertIntoStatement struct {
	TableSource string
	ColumnList  []string
	valueList   []SQLExprValue
}
