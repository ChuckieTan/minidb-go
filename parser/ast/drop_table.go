package ast

type DropTableStatement struct {
	tableName string
	ifExists  bool
}

func (statement DropTableStatement) StatementType() string {
	return "Drop table"
}
