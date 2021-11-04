package ast

type SQLStatement interface {
	StatementType() string
}
