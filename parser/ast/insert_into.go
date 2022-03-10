package ast

type InsertIntoStatement struct {
	TableName string
	Row       []SQLExprValue
}

func (statement InsertIntoStatement) StatementType() string {
	return "Insert into"
}
