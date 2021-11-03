package ast

type DeleteStatement struct {
	TableSource string
	Where       WhereStatement
}
