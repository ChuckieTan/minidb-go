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

func NewLexer(_sql string) (lexer *Lexer, err error) {
	lexer = &Lexer{
		sql:           strings.ToLower(_sql),
		tokenPos:      0,
		tokenSequence: make([]token.Token, 0, 16),
	}
	pos := 0
	for {
		resToken, chNum, err := lexer.scanToken(pos)
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

func (lexer *Lexer) scanSymbolToken(pos int) (token.Token, error) {
	if pos < len(lexer.sql)-1 {
		ch := lexer.sql[pos : pos+2]
		if tokenType, ok := symbolTokenType[ch]; ok {
			return token.Token{Type: tokenType, Val: ch}, nil
		}
	}

	ch := lexer.sql[pos : pos+1]
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

func (lexer *Lexer) scanLiteralToken(pos int) (resToken token.Token, err error) {
	tokenLen := 1
	ch := rune(lexer.sql[pos+tokenLen])
	for pos+tokenLen < len(lexer.sql) &&
		(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_') {
		tokenLen++
		ch = rune(lexer.sql[pos+tokenLen])
	}
	word := lexer.sql[pos : pos+tokenLen]

	if tokenType, ok := keywordTokenType[word]; ok {
		resToken = token.Token{Type: tokenType, Val: word}
	} else {
		resToken = token.Token{Type: token.TT_IDENTIFIER, Val: word}
	}

	return resToken, nil
}

func (lexer *Lexer) scanNumberToken(pos int) (resToken token.Token, err error) {
	tokenLen, numOfDot := 0, 0

	ch := rune(lexer.sql[pos+tokenLen])
	for pos+tokenLen < len(lexer.sql) &&
		(unicode.IsDigit(ch) || ch == '.') {
		if ch == '.' {
			numOfDot++
		}
		tokenLen++
		ch = rune(lexer.sql[pos+tokenLen])
	}
	word := lexer.sql[pos : pos+tokenLen]

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

func (lexer *Lexer) scanStringToken(pos int) (resToken token.Token, err error) {
	if lexer.sql[pos] != '\'' {
		err = fmt.Errorf("not a string")
		return
	}
	tokenLen := 1
	for lexer.sql[pos+tokenLen] != '\'' {
		tokenLen++
	}
	resToken = token.Token{Type: token.TT_STRING, Val: (lexer.sql)[pos+1 : pos+tokenLen]}
	return resToken, nil
}

func (lexer *Lexer) scanToken(pos int) (resToken token.Token, chNum int, err error) {
	if pos >= len(lexer.sql) {
		return token.Token{Type: token.TT_END, Val: ""}, chNum, nil
	}
	// 忽略空白字符
	for unicode.IsSpace(rune(lexer.sql[pos])) {
		pos++
		chNum++
	}
	ch := rune(lexer.sql[pos])
	switch {
	case unicode.IsLetter(ch):
		resToken, err = lexer.scanLiteralToken(pos)
	case unicode.IsDigit(ch):
		resToken, err = lexer.scanNumberToken(pos)
	case ch == '\'':
		resToken, err = lexer.scanStringToken(pos)
		chNum += 2
	default:
		resToken, err = lexer.scanSymbolToken(pos)
	}
	chNum += len(resToken.Val)
	return resToken, chNum, err
}
