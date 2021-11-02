package parser

import (
	"fmt"
	"minidb-go/ast"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type Parser struct {
	lexer Lexer
}

func NewParser(sql *string) (parser Parser, err error) {
	lexer, err := NewLexer(*sql)
	if err != nil {
		return parser, err
	}
	parser.lexer = lexer
	return parser, nil
}

func (parser *Parser) ParseCreateTableStatement() (
	statement ast.CreateTableStatement, err error,
) {
	if !parser.chain(TT_CREATE, TT_TABLE) {
		err = fmt.Errorf("not a create table statement")
		log.Error(err.Error())
		return statement, err
	}
	statement.TableName, err = parser.parseTableName()
	if err != nil {
		return statement, err
	}
	if !parser.match(TT_LBRACKET) {
		err = fmt.Errorf("expected a '('")
		log.Error(err.Error())
		return statement, err
	}
	for {
		define, err := parser.parseColumnDefine()
		statement.ColumnDeineList = append(statement.ColumnDeineList, define)
		if !parser.match(TT_COMMA) || err != nil {
			break
		}
	}
	if err != nil {
		log.Error(err.Error())
		return
	}
	if !parser.chain(TT_RBRACKET, TT_SEMICOLON) {
		err = fmt.Errorf("expected ')' or ';'")
		log.Error(err.Error())
	}
	return
}

func (parser *Parser) parseColumnDefine() (
	define ast.ColumnDefine, err error) {
	if parser.lexer.GetCurrentToken().Type != TT_IDENTIFIER {
		err = fmt.Errorf("expected a column name")
		log.Error(err.Error())
		return define, err
	}
	define.Name = parser.lexer.GetNextToken().Val

	token := parser.lexer.GetNextToken()
	if token.Type != TT_IDENTIFIER {
		err = fmt.Errorf("expected a column name")
		log.Error(err.Error())
		return define, err
	}
	if token.Val != "int" && token.Val != "float" && token.Val != "text" {
		err = fmt.Errorf("invalid column datatype: %v", token.Val)
		log.Error(err.Error())
		return define, err
	}
	define.SetColumnType(token.Val)
	return define, nil
}

func (parser *Parser) parseTableName() (tableName string, err error) {
	if parser.lexer.GetCurrentToken().Type != TT_IDENTIFIER {
		err = fmt.Errorf("expected a table name")
		log.Error(err.Error())
		return "", err
	}
	return parser.lexer.GetNextToken().Val, nil
}

func (parser *Parser) ParseInsertIntoStatement() (
	statement ast.InsertIntoStatement,
	err error,
) {
	if !parser.chain(TT_INSERT, TT_INTO) {
		err = fmt.Errorf("not a create table statement")
		log.Error(err.Error())
		return
	}
	if token := parser.lexer.GetNextToken(); token.Type == TT_IDENTIFIER {
		statement.TableSource = token.Val
	} else {
		err = fmt.Errorf("expect a table name, given %v", token.Val)
		log.Error(err.Error())
		return
	}
	if !parser.chain(TT_VALUES, TT_LBRACKET) {
		err = fmt.Errorf("expected 'values', '('")
		log.Error(err.Error())
		return
	}
	for {
		value, err := parser.parseLiteralValue()
		if err != nil {
			return statement, err
		}
		statement.ValueList = append(statement.ValueList, value)
		if !parser.match(TT_COMMA) {
			break
		}
	}
	if !parser.chain(TT_RBRACKET, TT_SEMICOLON) {
		err = fmt.Errorf("expected a '(' and ';'")
		log.Error(err.Error())
		return
	}
	return statement, nil
}

func (parser *Parser) parseLiteralValue() (
	value ast.SQLExprValue, err error,
) {
	token := parser.lexer.GetNextToken()
	switch token.Type {
	case TT_STRING:
		value = ast.SQLText(token.Val)
	case TT_PLUS:
		token = parser.lexer.GetNextToken()
		value, err = parser.parseNumericValue(1, token)
	case TT_MINUS:
		token = parser.lexer.GetNextToken()
		value, err = parser.parseNumericValue(-1, token)
	default:
		value, err = parser.parseNumericValue(1, token)
	}
	return value, err
}

func (parser *Parser) parseNumericValue(sign int, token Token) (
	value ast.SQLExprValue, err error,
) {
	switch token.Type {
	case TT_INTEGER:
		var v int64
		v, err = strconv.ParseInt(token.Val, 10, 64)
		if err != nil {
			err = fmt.Errorf("%v is not a int value", token.Val)
			log.Error(err.Error())
			return
		}
		return ast.SQLInt(int64(sign) * v), nil
	case TT_FLOAT:
		var v float64
		v, err = strconv.ParseFloat(token.Val, 64)
		if err != nil {
			err = fmt.Errorf("%v is not a int value", token.Val)
			log.Error(err.Error())
			return
		}
		return ast.SQLInt(float64(sign) * v), nil
	default:
		err = fmt.Errorf("expected a value, given '%v'", token.Val)
		log.Error(err.Error())
		return
	}
}

func (parser *Parser) match(tokenType TokenType) bool {
	savePoint := parser.lexer.mark()
	if parser.lexer.GetNextToken().Type == tokenType {
		return true
	} else {
		parser.lexer.reset(savePoint)
		return false
	}
}

func (parser *Parser) chain(tokenTypeList ...TokenType) bool {
	savePoint := parser.lexer.mark()
	for _, tokenType := range tokenTypeList {
		if !parser.match(TokenType(tokenType)) {
			parser.lexer.reset(savePoint)
			return false
		}
	}
	return true
}
