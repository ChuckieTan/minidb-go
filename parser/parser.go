package parser

import (
	"fmt"
	"minidb-go/ast"

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
	statement ast.CreateTableStatement, err error) {
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
