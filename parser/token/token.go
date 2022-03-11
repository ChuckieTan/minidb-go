package token

type TokenType int

const (
	TT_CREATE TokenType = iota
	TT_TABLE
	TT_INSERT
	TT_INTO
	TT_VALUES
	TT_DELETE
	TT_UPDATE
	TT_SET
	TT_DROP
	TT_SELECT
	TT_FROM
	TT_AS
	TT_WHERE
	TT_AND
	TT_OR
	TT_NOT
	TT_IDENTIFIER    // identifier
	TT_COMMA         //
	TT_STAR          // *
	TT_LBRACKET      // (
	TT_RBRACKET      // )
	TT_IN            // in
	TT_IF            // if
	TT_EXISTS        // exists
	TT_IS            // is
	TT_NULL_         // null
	TT_TRUE          // true
	TT_FALSE         // false
	TT_EQUAL         // ==
	TT_NOT_EQUAL     // '!=' <>
	TT_LESS          // <
	TT_LESS_EQUAL    // <=
	TT_GREATER       // >
	TT_GREATER_EQUAL // >=
	TT_PLUS          // +
	TT_MINUS         // -
	TT_INTEGER       // integer number
	TT_FLOAT         // float number
	TT_STRING        // string.
	TT_SEMICOLON     // ;
	TT_ILLEGAL       // illegal token
	TT_END           // end of SQL

	TT_DOT      // .
	TT_BETWEEN  // between
	TT_DISTINCT // distinct
	TT_DIV      // /
	TT_MOD      // %
	TT_ASSIGN   // =
	TT_ALL      // all

	TT_BEGIN
	TT_COMMIT
	TT_ROLLBACK
)

type Token struct {
	Type TokenType
	Val  string
}

func (tokenType TokenType) String() string {
	switch tokenType {
	case TT_CREATE:
		return "CREATE"
	case TT_TABLE:
		return "TABLE"
	case TT_INSERT:
		return "INSERT"
	case TT_INTO:
		return "INTO"
	case TT_VALUES:
		return "VALUES"
	case TT_DELETE:
		return "DELETE"
	case TT_UPDATE:
		return "UPDATE"
	case TT_SET:
		return "SET"
	case TT_DROP:
		return "DROP"
	case TT_SELECT:
		return "SELECT"
	case TT_FROM:
		return "FROM"
	case TT_AS:
		return "AS"
	case TT_WHERE:
		return "WHERE"
	case TT_AND:
		return "AND"
	case TT_OR:
		return "OR"
	case TT_NOT:
		return "NOT"
	case TT_IDENTIFIER:
		return "IDENTIFIER"
	case TT_COMMA:
		return "COMMA"
	case TT_STAR:
		return "STAR"
	case TT_LBRACKET:
		return "LBRACKET"
	case TT_RBRACKET:
		return "RBRACKET"
	case TT_IN:
		return "IN"
	case TT_IF:
		return "IF"
	case TT_EXISTS:
		return "EXISTS"
	case TT_IS:
		return "IS"
	case TT_NULL_:
		return "NULL"
	case TT_TRUE:
		return "TRUE"
	case TT_FALSE:
		return "FALSE"
	case TT_EQUAL:
		return "EQUAL"
	case TT_NOT_EQUAL:
		return "NOT_EQUAL"
	case TT_LESS:
		return "LESS"
	case TT_LESS_EQUAL:
		return "LESS_EQUAL"
	case TT_GREATER:
		return "GREATER"
	case TT_GREATER_EQUAL:
		return "GREATER_EQUAL"
	case TT_PLUS:
		return "PLUS"
	case TT_MINUS:
		return "MINUS"
	case TT_INTEGER:
		return "INTEGER"
	case TT_FLOAT:
		return "FLOAT"
	case TT_STRING:
		return "STRING"
	case TT_SEMICOLON:
		return "SEMICOLON"
	case TT_ILLEGAL:
		return "ILLEGAL"
	case TT_END:
		return "END"

	case TT_DOT:
		return "DOT"
	case TT_BETWEEN:
		return "BETWEEN"
	case TT_DISTINCT:
		return "DISTINCT"
	case TT_DIV:
		return "DIV"
	case TT_MOD:
		return "MOD"
	case TT_ASSIGN:
		return "ASSIGN"
	case TT_ALL:
		return "ALL"

	case TT_BEGIN:
		return "BEGIN"
	case TT_COMMIT:
		return "COMMIT"
	case TT_ROLLBACK:
		return "ROLLBACK"
	}
	return "UNKNOWN"
}
