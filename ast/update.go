package ast

type ColumnAssign struct {
	ColumnName string
	Value      SQLExprValue
}

type UpdateStatement struct {
	TableSource      string
	ColumnAssignList []ColumnAssign
	Where            WhereStatement
}
