package parser

import (
	"fmt"
	"minidb-go/parser/ast"
	"minidb-go/parser/token"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type Parser struct {
	lexer Lexer
}

func NewParser(sql string) (parser Parser, err error) {
	lexer, err := NewLexer(sql)
	if err != nil {
		return parser, err
	}
	parser.lexer = lexer
	return parser, nil
}

func (parser *Parser) ParseStatement() (
	statement ast.SQLStatement, err error,
) {
	savePoint := parser.lexer.mark()
	if parser.chain(token.TT_CREATE, token.TT_TABLE) {
		statement, err = parser.ParseCreateTableStatement()
		return
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_INSERT, token.TT_INTO) {
		statement, err = parser.ParseInsertIntoStatement()
		return
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_UPDATE) {
		statement, err = parser.ParseUpdateStatement()
		return
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_DELETE, token.TT_FROM) {
		statement, err = parser.ParseDeleteStatement()
		return
	}

	if parser.chain(token.TT_SELECT) {
		statement, err = parser.ParseSelectStatement()
		return
	}
	err = fmt.Errorf("expected a statement")
	return
}

func (parser *Parser) parseCreateIndexStatement() (
	statement ast.SQLStatement, err error,
) {
	return
}

func (parser *Parser) ParseCreateTableStatement() (
	statement ast.CreateTableStatement, err error,
) {
	statement.TableName, err = parser.parseTableName()
	if err != nil {
		return statement, err
	}
	if !parser.match(token.TT_LBRACKET) {
		err = fmt.Errorf("expected a '('")
		log.Error(err.Error())
		return statement, err
	}
	for {
		define, err := parser.parseColumnDefine()
		statement.ColumnDeineList = append(statement.ColumnDeineList, define)
		if !parser.match(token.TT_COMMA) || err != nil {
			break
		}
	}
	if err != nil {
		log.Error(err.Error())
		return
	}
	if !parser.chain(token.TT_RBRACKET, token.TT_SEMICOLON) {
		err = fmt.Errorf("expected ')' or ';'")
		log.Error(err.Error())
	}
	return
}

func (parser *Parser) parseColumnDefine() (
	define ast.ColumnDefine, err error) {
	if parser.lexer.GetCurrentToken().Type != token.TT_IDENTIFIER {
		err = fmt.Errorf("expected a column name")
		log.Error(err.Error())
		return define, err
	}
	define.Name = parser.lexer.GetNextToken().Val

	t := parser.lexer.GetNextToken()
	if t.Type != token.TT_IDENTIFIER {
		err = fmt.Errorf("expected a column name")
		log.Error(err.Error())
		return define, err
	}
	if t.Val != "int" && t.Val != "float" && t.Val != "text" {
		err = fmt.Errorf("invalid column datatype: %v", t.Val)
		log.Error(err.Error())
		return define, err
	}
	define.SetColumnType(t.Val)
	return define, nil
}

func (parser *Parser) parseTableName() (tableName string, err error) {
	if parser.lexer.GetCurrentToken().Type != token.TT_IDENTIFIER {
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
	if t := parser.lexer.GetNextToken(); t.Type == token.TT_IDENTIFIER {
		statement.TableName = t.Val
	} else {
		err = fmt.Errorf("expect a table name, given %v", t.Val)
		log.Error(err.Error())
		return
	}
	if !parser.chain(token.TT_VALUES, token.TT_LBRACKET) {
		err = fmt.Errorf("expected 'values', '('")
		log.Error(err.Error())
		return
	}
	for {
		value, err := parser.parseLiteralValue()
		if err != nil {
			return statement, err
		}
		statement.Row = append(statement.Row, value)
		if !parser.match(token.TT_COMMA) {
			break
		}
	}
	if !parser.chain(token.TT_RBRACKET, token.TT_SEMICOLON) {
		err = fmt.Errorf("expected a '(' and ';'")
		log.Error(err.Error())
		return
	}
	return statement, nil
}

func (parser *Parser) ParseSelectStatement() (
	statement ast.SelectStatement,
	err error,
) {
	if parser.match(token.TT_STAR) {
		statement.ResultList = append(statement.ResultList, "*")
	} else {
		for {
			name, err := parser.parseColumnName()
			if err != nil {
				return statement, err
			}
			statement.ResultList = append(statement.ResultList, name)

			if !parser.match(token.TT_COMMA) {
				break
			}
		}
	}
	if t := parser.lexer.GetCurrentToken(); !parser.match(token.TT_FROM) {
		err = fmt.Errorf("expected 'from', found '%v'", t.Val)
		log.Error(err.Error())
	}

	if t := parser.lexer.GetCurrentToken(); parser.match(token.TT_IDENTIFIER) {
		statement.TableName = t.Val
	} else {
		err = fmt.Errorf("expected 'Identifier', found '%v'", t.Val)
		log.Error(err.Error())
		return
	}

	if t := parser.lexer.GetCurrentToken(); t.Type == token.TT_WHERE {
		where, err := parser.parseWhere()
		if err != nil {
			return statement, err
		}
		statement.Where = where
	} else {
		statement.Where.IsExists = false
	}

	if !parser.chain(token.TT_SEMICOLON) {
		err = fmt.Errorf("expected ';'")
		log.Error(err.Error())
		return
	}
	return statement, nil
}

func (parser *Parser) ParseUpdateStatement() (
	statement ast.UpdateStatement, err error) {
	statement.TableSource, err = parser.parseTableName()
	if err != nil {
		return
	}

	if t := parser.lexer.GetCurrentToken(); !parser.match(token.TT_SET) {
		err = fmt.Errorf("expected 'set', found '%v'", t.Val)
		log.Error(err.Error())
		return
	}

	// 循环获取赋值
	for {
		columnAssign, err := parser.parseColumnAssign()
		if err != nil {
			return statement, err
		}
		statement.ColumnAssignList = append(statement.ColumnAssignList, columnAssign)

		if !parser.match(token.TT_COMMA) {
			break
		}
	}

	if t := parser.lexer.GetCurrentToken(); t.Type != token.TT_WHERE {
		err = fmt.Errorf("expected 'where', found '%v'", t.Val)
		log.Error(err.Error())
		return
	}

	statement.Where, err = parser.parseWhere()

	if err != nil {
		return
	}

	return statement, err
}

func (parser *Parser) ParseDeleteStatement() (
	statement ast.DeleteStatement, err error,
) {
	statement.TableSource, err = parser.parseTableName()
	if err != nil {
		return
	}

	statement.Where, err = parser.parseWhere()
	if err != nil {
		return
	}
	return statement, err
}

func (parser *Parser) parseColumnAssign() (
	columnAssign ast.ColumnAssign, err error,
) {
	if t := parser.lexer.GetCurrentToken(); parser.match(token.TT_IDENTIFIER) {
		columnAssign.ColumnName = t.Val
	} else {
		err = fmt.Errorf("expected table name, found '%v'", t.Val)
		log.Error(err.Error())
		return
	}

	if t := parser.lexer.GetCurrentToken(); !parser.match(token.TT_ASSIGN) {
		err = fmt.Errorf("expected '=', found '%v'", t.Val)
		log.Error(err.Error())
		return
	}

	columnAssign.Value, err = parser.parseExprValue()

	if err != nil {
		return
	}

	return columnAssign, err
}

func (parser *Parser) parseWhere() (
	statement ast.WhereStatement, err error,
) {
	if t := parser.lexer.GetNextToken(); t.Type == token.TT_WHERE {
		statement.Expr, err = parser.parseExpr()
		statement.IsExists = true
		return
	} else {
		err = fmt.Errorf("expected 'where', found '%v'", t.Val)
		log.Error(err.Error())
		return
	}
}

func (parser *Parser) parseExpr() (
	expr *ast.SQLExpr, err error,
) {
	expr.Left, err = parser.parseExprValue()
	if err != nil {
		return
	}
	expr.Op, err = parser.parseComparisonOperator()
	if err != nil {
		return
	}
	expr.Right, err = parser.parseExprValue()
	if err != nil {
		return
	}
	return expr, nil
}

func (parse *Parser) parseExprValue() (
	exprValue ast.SQLExprValue, err error,
) {
	resToken := parse.lexer.GetCurrentToken()
	if resToken.Type == token.TT_IDENTIFIER {
		parse.lexer.GetNextToken()
		exprValue = ast.SQLColumn(resToken.Val)
		return
	} else {
		exprValue, err = parse.parseLiteralValue()
		return
	}
}

func (parse *Parser) parseComparisonOperator() (
	resType token.TokenType, err error) {
	if resToken := parse.lexer.GetCurrentToken(); parse.tree(
		token.TT_LESS, token.TT_LESS_EQUAL, token.TT_ASSIGN,
		token.TT_EQUAL, token.TT_NOT_EQUAL, token.TT_GREATER,
		token.TT_GREATER_EQUAL) {
		return resToken.Type, nil
	} else {
		err = fmt.Errorf("expected 'comparison operator, found '%v'", resToken.Val)
		log.Error(err.Error())
		return
	}
}

func (parser *Parser) parseColumnName() (name string, err error) {
	if t := parser.lexer.GetCurrentToken(); parser.match(token.TT_IDENTIFIER) {
		return t.Val, nil
	} else {
		err = fmt.Errorf("expected a column name, found '%v'", t.Val)
		log.Error(err.Error())
		return name, err
	}
}

func (parser *Parser) parseLiteralValue() (
	value ast.SQLExprValue, err error,
) {
	t := parser.lexer.GetNextToken()
	switch t.Type {
	case token.TT_STRING:
		value = ast.SQLText(t.Val)
	case token.TT_PLUS:
		t = parser.lexer.GetNextToken()
		value, err = parser.parseNumericValue(1, t)
	case token.TT_MINUS:
		t = parser.lexer.GetNextToken()
		value, err = parser.parseNumericValue(-1, t)
	default:
		value, err = parser.parseNumericValue(1, t)
	}
	return value, err
}

func (parser *Parser) parseNumericValue(sign int, numToken token.Token) (
	value ast.SQLExprValue, err error,
) {
	switch numToken.Type {
	case token.TT_INTEGER:
		var v int64
		v, err = strconv.ParseInt(numToken.Val, 10, 64)
		if err != nil {
			err = fmt.Errorf("%v is not a int value", numToken.Val)
			log.Error(err.Error())
			return
		}
		return ast.SQLInt(int64(sign) * v), nil
	case token.TT_FLOAT:
		var v float64
		v, err = strconv.ParseFloat(numToken.Val, 64)
		if err != nil {
			err = fmt.Errorf("%v is not a int value", numToken.Val)
			log.Error(err.Error())
			return
		}
		return ast.SQLFloat(float64(sign) * v), nil
	default:
		err = fmt.Errorf("expected a value, given '%v'", numToken.Val)
		log.Error(err.Error())
		return
	}
}

func (parser *Parser) match(tokenType token.TokenType) bool {
	savePoint := parser.lexer.mark()
	if parser.lexer.GetNextToken().Type == tokenType {
		return true
	} else {
		parser.lexer.reset(savePoint)
		return false
	}
}

func (parser *Parser) chain(tokenTypeList ...token.TokenType) bool {
	savePoint := parser.lexer.mark()
	for _, tokenType := range tokenTypeList {
		if !parser.match(token.TokenType(tokenType)) {
			parser.lexer.reset(savePoint)
			return false
		}
	}
	return true
}

func (parser *Parser) tree(tokenTypeList ...token.TokenType) bool {
	for _, tokenType := range tokenTypeList {
		if parser.match(tokenType) {
			return true
		}
	}
	return false
}
