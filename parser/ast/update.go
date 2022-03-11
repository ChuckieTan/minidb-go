package ast

type ColumnAssign struct {
	ColumnName string
	Value      SQLExprValue
}

type UpdateStmt struct {
	TableName        string
	ColumnAssignList []ColumnAssign
	Where            WhereStatement
}

func (statement UpdateStmt) StatementType() string {
	return "Update"
}
