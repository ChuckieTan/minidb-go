package ast

type SelectStatement struct {
	ResultList  []string
	TableSource string
	Where       WhereStatement
}

func (statement SelectStatement) StatementType() string {
	return "Select"
}
