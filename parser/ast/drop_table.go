package ast

type DropTableStatement struct {
	tableName string
	ifExists  bool
}
