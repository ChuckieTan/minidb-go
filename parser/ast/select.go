package ast

type SelectStmt struct {
	ResultColumn []string
	TableName    string
	Where        WhereStatement
}

func (statement SelectStmt) StatementType() string {
	return "Select"
}
