package ast

type SelectStatement struct {
	ResultList []string
	TableName  string
	Where      WhereStatement
}

func (statement SelectStatement) StatementType() string {
	return "Select"
}
