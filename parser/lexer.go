package parser

import (
	"fmt"
	"minidb-go/parser/token"
	"strings"
	"unicode"
)

type Lexer struct {
	sql           string
	tokenPos      int
	tokenSequence []token.Token
}

type savePoint struct {
	tokenPos int
}

func (lexer *Lexer) mark() savePoint {
	return savePoint{
		tokenPos: lexer.tokenPos,
	}
}

func (lexer *Lexer) reset(savePoint savePoint) {
	lexer.tokenPos = savePoint.tokenPos
}

func NewLexer(_sql string) (lexer Lexer, err error) {
	lexer = Lexer{sql: _sql, tokenPos: 0, tokenSequence: make([]token.Token, 0)}
	pos := 0
	for {
		resToken, chNum, err := scanToken(lexer.sql, pos)
		if resToken.Type == token.TT_END || resToken.Type == token.TT_ILLEGAL {
			break
		}
		if err == nil {
			pos += chNum
			lexer.tokenSequence = append(lexer.tokenSequence, resToken)
		} else {
			return lexer, fmt.Errorf("invalid sql statement")
		}
	}
	return lexer, nil
}

func (lexer *Lexer) GetNextToken() (resToken token.Token) {
	if lexer.tokenPos < len(lexer.tokenSequence) {
		resToken = lexer.tokenSequence[lexer.tokenPos]
		lexer.tokenPos++
	} else {
		resToken = token.Token{Type: token.TT_END, Val: ""}
	}
	return resToken
}

func (lexer *Lexer) GetCurrentToken() (resToken token.Token) {
	if lexer.tokenPos < len(lexer.tokenSequence) {
		resToken = lexer.tokenSequence[lexer.tokenPos]
	} else {
		resToken = token.Token{Type: token.TT_END, Val: ""}
	}
	return resToken
}

var symbolTokenType = map[string]token.TokenType{
	",":  token.TT_COMMA,
	"*":  token.TT_STAR,
	"(":  token.TT_LBRACKET,
	")":  token.TT_RBRACKET,
	"+":  token.TT_PLUS,
	"-":  token.TT_MINUS,
	";":  token.TT_SEMICOLON,
	"==": token.TT_EQUAL,
	"!=": token.TT_NOT_EQUAL,
	"<=": token.TT_LESS_EQUAL,
	"<>": token.TT_NOT_EQUAL,
	">=": token.TT_GREATER_EQUAL,
	"=":  token.TT_ASSIGN,
	".":  token.TT_DOT,
	"/":  token.TT_DIV,
	"%":  token.TT_MOD,
}

func scanSymbolToken(sql string, pos int) (token.Token, error) {
	if pos < len(sql)-1 {
		ch := sql[pos : pos+2]
		if tokenType, ok := symbolTokenType[ch]; ok {
			return token.Token{Type: tokenType, Val: ch}, nil
		}
	}

	ch := sql[pos : pos+1]
	if tokenType, ok := symbolTokenType[ch]; ok {
		return token.Token{Type: tokenType, Val: ch}, nil
	}

	token := token.Token{Type: token.TT_ILLEGAL, Val: ""}
	err := fmt.Errorf("cannot scan symbol token which is %v", ch)
	return token, err
}

var keywordTokenType = map[string]token.TokenType{
	"create":   token.TT_CREATE,
	"table":    token.TT_TABLE,
	"insert":   token.TT_INSERT,
	"into":     token.TT_INTO,
	"values":   token.TT_VALUES,
	"delete":   token.TT_DELETE,
	"update":   token.TT_UPDATE,
	"set":      token.TT_SET,
	"drop":     token.TT_DROP,
	"select":   token.TT_SELECT,
	"from":     token.TT_FROM,
	"where":    token.TT_WHERE,
	"and":      token.TT_AND,
	"or":       token.TT_OR,
	"not":      token.TT_NOT,
	"in":       token.TT_IN,
	"is":       token.TT_IS,
	"null":     token.TT_NULL_,
	"if":       token.TT_IF,
	"exists":   token.TT_EXISTS,
	"true":     token.TT_TRUE,
	"false":    token.TT_FALSE,
	"between":  token.TT_BETWEEN,
	"distinct": token.TT_DISTINCT,
	"all":      token.TT_ALL,
	"begin":    token.TT_BEGIN,
	"commit":   token.TT_COMMIT,
	"rollback": token.TT_ROLLBACK,
}

func scanLiteralToken(sql string, pos int) (resToken token.Token, err error) {
	tokenLen := 1
	ch := rune(sql[pos+tokenLen])
	for pos+tokenLen < len(sql) &&
		(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_') {
		tokenLen++
		ch = rune(sql[pos+tokenLen])
	}
	word := sql[pos : pos+tokenLen]
	word = strings.ToLower(word)

	if tokenType, ok := keywordTokenType[word]; ok {
		resToken = token.Token{Type: tokenType, Val: word}
	} else {
		resToken = token.Token{Type: token.TT_IDENTIFIER, Val: word}
	}

	return resToken, nil
}

func scanNumberToken(sql string, pos int) (resToken token.Token, err error) {
	tokenLen, numOfDot := 0, 0

	ch := rune(sql[pos+tokenLen])
	for pos+tokenLen < len(sql) &&
		(unicode.IsDigit(ch) || ch == '.') {
		if ch == '.' {
			numOfDot++
		}
		tokenLen++
		ch = rune(sql[pos+tokenLen])
	}
	word := sql[pos : pos+tokenLen]

	switch numOfDot {
	case 0:
		resToken = token.Token{Type: token.TT_INTEGER, Val: word}
		err = nil
	case 1:
		resToken = token.Token{Type: token.TT_FLOAT, Val: word}
		err = nil
	default:
		resToken = token.Token{Type: token.TT_ILLEGAL, Val: word}
		err = fmt.Errorf("%v is not a number", word)
	}

	return resToken, err
}

func scanStringToken(sql string, pos int) (
	resToken token.Token,
	err error,
) {
	if sql[pos] != '\'' {
		err = fmt.Errorf("not a string")
		return
	}
	tokenLen := 1
	for sql[pos+tokenLen] != '\'' {
		tokenLen++
	}
	resToken = token.Token{Type: token.TT_STRING, Val: (sql)[pos+1 : pos+tokenLen]}
	return resToken, nil
}

func scanToken(sql string, pos int) (resToken token.Token, chNum int, err error) {
	if pos >= len(sql) {
		return token.Token{Type: token.TT_END, Val: ""}, chNum, nil
	}
	// 忽略空白字符
	for unicode.IsSpace(rune(sql[pos])) {
		pos++
		chNum++
	}
	ch := rune(sql[pos])
	switch {
	case unicode.IsDigit(ch):
		resToken, err = scanNumberToken(sql, pos)
	case unicode.IsLetter(ch):
		resToken, err = scanLiteralToken(sql, pos)
	case ch == '\'':
		resToken, err = scanStringToken(sql, pos)
		chNum += 2
	default:
		resToken, err = scanSymbolToken(sql, pos)
	}
	chNum += len(resToken.Val)
	return resToken, chNum, err
}
