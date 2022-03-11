package ast

type InsertIntoStmt struct {
	TableName string
	Row       []SQLExprValue
}

func (statement InsertIntoStmt) StatementType() string {
	return "Insert into"
}
