package ast

type SQLInt int64
type SQLFloat float64
type SQLText string
type SQLColumn string

type SQLExprValue interface {
	isExpr() bool
}

func (sqlInt SQLInt) isExpr() bool {
	return true
}
func (sqlFloat SQLFloat) isExpr() bool {
	return true
}
func (sqlText SQLText) isExpr() bool {
	return true
}
func (sqlColumn SQLColumn) isExpr() bool {
	return true
}

type SQLExpr struct {
	LValue SQLExprValue
	Op     string
	RValue SQLExprValue
}
