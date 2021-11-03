package ast

type InsertIntoStatement struct {
	TableSource string
	ValueList   []SQLExprValue
}
