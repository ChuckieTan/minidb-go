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
)

type Token struct {
	Type TokenType
	Val  string
}
