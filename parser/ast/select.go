package ast

type SelectStatement struct {
	ResultList  []string
	TableSource string
	Where       WhereStatement
}
