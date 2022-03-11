package ast

type SelectStatement struct {
	ResultColumn []string
	TableName    string
	Where        WhereStatement
}

func (statement SelectStatement) StatementType() string {
	return "Select"
}
