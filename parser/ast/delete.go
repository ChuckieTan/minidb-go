package ast

type DeleteStatement struct {
	TableSource string
	Where       WhereStatement
}

func (statement DeleteStatement) StatementType() string {
	return "Delete"
}
