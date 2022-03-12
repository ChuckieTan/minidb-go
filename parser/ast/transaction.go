package ast

type Transaction struct {
	statements []SQLStatement
}

type BeginStmt struct {
}

func (stmt BeginStmt) StatementType() string {
	return "begin"
}

type CommitStmt struct {
}

func (stmt CommitStmt) StatementType() string {
	return "commit"
}

type RollbackStmt struct {
}

func (stmt RollbackStmt) StatementType() string {
	return "rollback"
}
