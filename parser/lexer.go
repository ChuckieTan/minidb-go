package parser

import (
	"fmt"
	"strings"
	"unicode"
)

type Lexer struct {
	sql           string
	tokenPos      int
	tokenSequence []Token
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
	lexer = Lexer{sql: _sql, tokenPos: 0, tokenSequence: make([]Token, 0)}
	pos := 0
	for {
		token, chNum, err := scanToken(&lexer.sql, pos)
		if token.Type == TT_END || token.Type == TT_ILLEGAL {
			break
		}
		if err == nil {
			pos += chNum
			lexer.tokenSequence = append(lexer.tokenSequence, token)
		} else {
			return lexer, fmt.Errorf("invalid sql statement")
		}
	}
	return lexer, nil
}

func (lexer *Lexer) GetNextToken() (token Token) {
	if lexer.tokenPos < len(lexer.tokenSequence) {
		token = lexer.tokenSequence[lexer.tokenPos]
		lexer.tokenPos++
	} else {
		token = Token{Type: TT_END, Val: ""}
	}
	return token
}

func (lexer *Lexer) GetCurrentToken() (token Token) {
	if lexer.tokenPos < len(lexer.tokenSequence) {
		token = lexer.tokenSequence[lexer.tokenPos]
	} else {
		token = Token{Type: TT_END, Val: ""}
	}
	return token
}

var symbolTokenType = map[string]TokenType{
	",":  TT_COMMA,
	"*":  TT_STAR,
	"(":  TT_LBRACKET,
	")":  TT_RBRACKET,
	"+":  TT_PLUS,
	"-":  TT_MINUS,
	";":  TT_SEMICOLON,
	"==": TT_EQUAL,
	"!=": TT_NOT_EQUAL,
	"<=": TT_LESS_OR_EQUAL,
	"<>": TT_NOT_EQUAL,
	">=": TT_GREATER_OR_EQUAL,
	"=":  TT_ASSIGN,
	".":  TT_DOT,
	"/":  TT_DIV,
	"%":  TT_MOD,
}

func scanSymbolToken(sql *string, pos int) (Token, error) {
	if pos < len(*sql)-1 {
		ch := (*sql)[pos : pos+2]
		if tokenType, ok := symbolTokenType[ch]; ok {
			return Token{Type: tokenType, Val: ch}, nil
		}
	}

	ch := (*sql)[pos : pos+1]
	if tokenType, ok := symbolTokenType[ch]; ok {
		return Token{Type: tokenType, Val: ch}, nil
	}

	token := Token{Type: TT_ILLEGAL, Val: ""}
	err := fmt.Errorf("cannot scan symbol token which is %v", ch)
	return token, err
}

var keywordTokenType = map[string]TokenType{
	"create":   TT_CREATE,
	"table":    TT_TABLE,
	"insert":   TT_INSERT,
	"into":     TT_INTO,
	"values":   TT_VALUES,
	"delete":   TT_DELETE,
	"update":   TT_UPDATE,
	"set":      TT_SET,
	"drop":     TT_DROP,
	"select":   TT_SELECT,
	"from":     TT_FROM,
	"where":    TT_WHERE,
	"and":      TT_AND,
	"or":       TT_OR,
	"not":      TT_NOT,
	"in":       TT_IN,
	"is":       TT_IS,
	"null":     TT_NULL_,
	"if":       TT_IF,
	"exists":   TT_EXISTS,
	"true":     TT_TRUE,
	"false":    TT_FALSE,
	"between":  TT_BETWEEN,
	"distinct": TT_DISTINCT,
	"all":      TT_ALL,
}

func scanLiteralToken(sql *string, pos int) (token Token, err error) {
	tokenLen := 1
	ch := rune((*sql)[pos+tokenLen])
	for pos+tokenLen < len(*sql) &&
		(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_') {
		tokenLen++
		ch = rune((*sql)[pos+tokenLen])
	}
	word := (*sql)[pos : pos+tokenLen]
	word = strings.ToLower(word)

	if tokenType, ok := keywordTokenType[word]; ok {
		token = Token{Type: tokenType, Val: word}
	} else {
		token = Token{Type: TT_IDENTIFIER, Val: word}
	}

	return token, nil
}

func scanNumberToken(sql *string, pos int) (token Token, err error) {
	tokenLen, numOfDot := 0, 0

	ch := rune((*sql)[pos+tokenLen])
	for pos+tokenLen < len(*sql) &&
		(unicode.IsDigit(ch) || ch == '.') {
		if ch == '.' {
			numOfDot++
		}
		tokenLen++
		ch = rune((*sql)[pos+tokenLen])
	}
	word := (*sql)[pos : pos+tokenLen]

	switch numOfDot {
	case 0:
		token = Token{Type: TT_INTEGER, Val: word}
		err = nil
	case 1:
		token = Token{Type: TT_FLOAT, Val: word}
		err = nil
	default:
		token = Token{Type: TT_ILLEGAL, Val: word}
		err = fmt.Errorf("%v is not a number", word)
	}

	return token, err
}

func scanStringToken(sql *string, pos int) (
	token Token,
	err error,
) {
	if (*sql)[pos] != '\'' {
		err = fmt.Errorf("not a string")
		return
	}
	tokenLen := 1
	for (*sql)[pos+tokenLen] != '\'' {
		tokenLen++
	}
	token = Token{Type: TT_STRING, Val: (*sql)[pos+1 : pos+tokenLen]}
	return token, nil
}

func scanToken(sql *string, pos int) (token Token, chNum int, err error) {
	if pos >= len(*sql) {
		return Token{Type: TT_END, Val: ""}, chNum, nil
	}
	// 忽略空白字符
	for unicode.IsSpace(rune((*sql)[pos])) {
		pos++
		chNum++
	}
	ch := rune((*sql)[pos])
	switch {
	case unicode.IsDigit(ch):
		token, err = scanNumberToken(sql, pos)
	case unicode.IsLetter(ch):
		token, err = scanLiteralToken(sql, pos)
	case ch == '\'':
		token, err = scanStringToken(sql, pos)
		chNum += 2
	default:
		token, err = scanSymbolToken(sql, pos)
	}
	chNum += len(token.Val)
	return token, chNum, err
}
