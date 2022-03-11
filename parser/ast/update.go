package ast

type ColumnAssign struct {
	ColumnName string
	Value      SQLExprValue
}

type UpdateStatement struct {
	TableName        string
	ColumnAssignList []ColumnAssign
	Where            WhereStatement
}

func (statement UpdateStatement) StatementType() string {
	return "Update"
}
