package ast

type WhereStatement struct {
	IsExists bool
	Expr     SQLExpr
}
