package ast

type Transaction struct {
	statements []SQLStatement
}

type SQLBeginTransaction struct {
}

func (stmt *SQLBeginTransaction) StatementType() string {
	return "begin"
}

type SQLCommitTransaction struct {
}

func (stmt *SQLCommitTransaction) StatementType() string {
	return "commit"
}

type SQLRollbackTransaction struct {
}

func (stmt *SQLRollbackTransaction) StatementType() string {
	return "rollback"
}
