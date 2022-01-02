package ast

type InsertIntoStatement struct {
	TableName string
	Row       Row
}

func (statement InsertIntoStatement) StatementType() string {
	return "Insert into"
}
