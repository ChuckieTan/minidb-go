package ast

type WhereStatement struct {
	isExists bool
	expr     SQLExpr
}
