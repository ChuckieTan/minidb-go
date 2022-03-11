package ast

type DeleteStatement struct {
	TableName string
	Where     WhereStatement
}

func (statement DeleteStatement) StatementType() string {
	return "Delete"
}
