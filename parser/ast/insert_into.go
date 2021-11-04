package ast

type InsertIntoStatement struct {
	TableSource string
	ValueList   []SQLExprValue
}

func (statement InsertIntoStatement) StatementType() string {
	return "Insert into"
}
