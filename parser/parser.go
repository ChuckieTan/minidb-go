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

func NewParser(sql string) (*Parser, error) {
	lexer, err := NewLexer(sql)
	if err != nil {
		return nil, err
	}
	parser := &Parser{
		lexer: lexer,
	}
	return parser, nil
}

func Parse(sql string) (ast.SQLStatement, error) {
	parser, err := NewParser(sql)
	if err != nil {
		return nil, err
	}
	return parser.ParseStatement()
}

func (parser *Parser) ParseStatement() (ast.SQLStatement, error) {
	savePoint := parser.lexer.mark()
	if parser.chain(token.TT_BEGIN, token.TT_SEMICOLON) {
		return ast.BeginStmt{}, nil
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_COMMIT, token.TT_SEMICOLON) {
		return ast.CommitStmt{}, nil
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_ROLLBACK, token.TT_SEMICOLON) {
		return ast.RollbackStmt{}, nil
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_CREATE, token.TT_TABLE) {
		return parser.ParseCreateTableStatement()
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_INSERT, token.TT_INTO) {
		return parser.ParseInsertIntoStatement()
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_UPDATE) {
		return parser.ParseUpdateStatement()
	}

	parser.lexer.reset(savePoint)
	if parser.chain(token.TT_DELETE, token.TT_FROM) {
		return parser.ParseDeleteStatement()
	}

	if parser.chain(token.TT_SELECT) {
		return parser.ParseSelectStatement()
	}
	return nil, fmt.Errorf("expected a statement")
}

func (parser *Parser) parseTransaction() {

}

func (parser *Parser) parseCreateIndexStatement() (ast.SQLStatement, error) {
	return nil, nil
}

func (parser *Parser) ParseCreateTableStatement() (ast.CreateTableStmt, error) {
	var err error
	statement := ast.CreateTableStmt{
		ColumnDefines: make([]*ast.ColumnDefine, 0),
	}
	statement.TableName, err = parser.parseTableName()
	if err != nil {
		return statement, err
	}
	if !parser.match(token.TT_LBRACKET) {
		err = fmt.Errorf("expected a '('")
		log.Error(err.Error())
		return statement, err
	}
	i := uint16(0)
	for {
		define, err := parser.parseColumnDefine()
		define.ColumnId = i
		i++
		statement.ColumnDefines = append(statement.ColumnDefines, define)
		if !parser.match(token.TT_COMMA) || err != nil {
			break
		}
	}
	if err != nil {
		log.Error(err.Error())
		return statement, err
	}
	if !parser.chain(token.TT_RBRACKET, token.TT_SEMICOLON) {
		err = fmt.Errorf("expected ')' or ';'")
		log.Error(err.Error())
	}
	return statement, err
}

func (parser *Parser) parseColumnDefine() (*ast.ColumnDefine, error) {
	define := new(ast.ColumnDefine)
	var err error
	if parser.lexer.GetCurrentToken().Type != token.TT_IDENTIFIER {
		err = fmt.Errorf("expected a column name")
		log.Error(err)
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

func (parser *Parser) parseTableName() (string, error) {
	if parser.lexer.GetCurrentToken().Type != token.TT_IDENTIFIER {
		err := fmt.Errorf("expected a table name")
		log.Error(err.Error())
		return "", err
	}
	return parser.lexer.GetNextToken().Val, nil
}

func (parser *Parser) ParseInsertIntoStatement() (ast.InsertIntoStmt, error) {
	stmt := ast.InsertIntoStmt{
		TableName: "",
		Row:       make([]ast.SQLExprValue, 0),
	}
	if t := parser.lexer.GetNextToken(); t.Type == token.TT_IDENTIFIER {
		stmt.TableName = t.Val
	} else {
		err := fmt.Errorf("expect a table name, given %v", t.Val)
		log.Error(err.Error())
		return stmt, err
	}
	if !parser.chain(token.TT_VALUES, token.TT_LBRACKET) {
		err := fmt.Errorf("expected 'values', '('")
		log.Error(err.Error())
		return stmt, err
	}
	for {
		value, err := parser.parseLiteralValue()
		if err != nil {
			return stmt, err
		}
		stmt.Row = append(stmt.Row, value)
		if !parser.match(token.TT_COMMA) {
			break
		}
	}
	if !parser.chain(token.TT_RBRACKET, token.TT_SEMICOLON) {
		err := fmt.Errorf("expected a '(' and ';'")
		log.Error(err.Error())
		return stmt, err
	}
	return stmt, nil
}

func (parser *Parser) ParseSelectStatement() (ast.SelectStmt, error) {
	stmt := ast.SelectStmt{}
	var err error
	if parser.match(token.TT_STAR) {
		stmt.ResultColumn = append(stmt.ResultColumn, "*")
	} else {
		for {
			name, err := parser.parseColumnName()
			if err != nil {
				return stmt, err
			}
			stmt.ResultColumn = append(stmt.ResultColumn, name)

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
		stmt.TableName = t.Val
	} else {
		err = fmt.Errorf("expected 'Identifier', found '%v'", t.Val)
		log.Error(err.Error())
		return stmt, err
	}

	if t := parser.lexer.GetCurrentToken(); t.Type == token.TT_WHERE {
		where, err := parser.parseWhere()
		if err != nil {
			return stmt, err
		}
		stmt.Where = where
	} else {
		stmt.Where.IsExists = false
	}

	if !parser.chain(token.TT_SEMICOLON) {
		err = fmt.Errorf("expected ';'")
		log.Error(err.Error())
		return stmt, err
	}
	return stmt, nil
}

func (parser *Parser) ParseUpdateStatement() (ast.UpdateStmt, error) {
	stmt := ast.UpdateStmt{}
	var err error
	stmt.TableName, err = parser.parseTableName()
	if err != nil {
		return stmt, err
	}

	if t := parser.lexer.GetCurrentToken(); !parser.match(token.TT_SET) {
		err = fmt.Errorf("expected 'set', found '%v'", t.Val)
		log.Error(err.Error())
		return stmt, err
	}

	// 循环获取赋值
	for {
		columnAssign, err := parser.parseColumnAssign()
		if err != nil {
			return stmt, err
		}
		stmt.ColumnAssignList = append(stmt.ColumnAssignList, columnAssign)

		if !parser.match(token.TT_COMMA) {
			break
		}
	}

	if t := parser.lexer.GetCurrentToken(); t.Type != token.TT_WHERE {
		err = fmt.Errorf("expected 'where', found '%v'", t.Val)
		log.Error(err.Error())
		return stmt, err
	}

	stmt.Where, err = parser.parseWhere()

	if err != nil {
		return stmt, err
	}

	return stmt, err
}

func (parser *Parser) ParseDeleteStatement() (ast.DeleteStatement, error) {
	stmt := ast.DeleteStatement{}
	var err error
	stmt.TableName, err = parser.parseTableName()
	if err != nil {
		return stmt, err
	}

	stmt.Where, err = parser.parseWhere()
	if err != nil {
		return stmt, err
	}
	return stmt, err
}

func (parser *Parser) parseColumnAssign() (ast.ColumnAssign, error) {
	columnAssign := ast.ColumnAssign{}
	var err error
	if t := parser.lexer.GetCurrentToken(); parser.match(token.TT_IDENTIFIER) {
		columnAssign.ColumnName = t.Val
	} else {
		err = fmt.Errorf("expected table name, found '%v'", t.Val)
		log.Error(err.Error())
		return columnAssign, err
	}

	if t := parser.lexer.GetCurrentToken(); !parser.match(token.TT_ASSIGN) {
		err = fmt.Errorf("expected '=', found '%v'", t.Val)
		log.Error(err.Error())
		return columnAssign, err
	}

	columnAssign.Value, err = parser.parseExprValue()

	if err != nil {
		return columnAssign, err
	}

	return columnAssign, err
}

func (parser *Parser) parseWhere() (ast.WhereStatement, error) {
	stmt := ast.WhereStatement{}
	var err error
	if t := parser.lexer.GetNextToken(); t.Type == token.TT_WHERE {
		stmt.Expr, err = parser.parseExpr()
		stmt.IsExists = true
		return stmt, err
	} else {
		err = fmt.Errorf("expected 'where', found '%v'", t.Val)
		log.Error(err.Error())
		return stmt, err
	}
}

func (parser *Parser) parseExpr() (*ast.SQLExpr, error) {
	expr := &ast.SQLExpr{}
	var err error
	expr.Left, err = parser.parseExprValue()
	if err != nil {
		return nil, err
	}
	expr.Op, err = parser.parseComparisonOperator()
	if err != nil {
		return nil, err
	}
	expr.Right, err = parser.parseExprValue()
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (parse *Parser) parseExprValue() (ast.SQLExprValue, error) {
	resToken := parse.lexer.GetCurrentToken()
	if resToken.Type == token.TT_IDENTIFIER {
		parse.lexer.GetNextToken()
		column := ast.SQLColumn(resToken.Val)
		return &column, nil
	} else {
		return parse.parseLiteralValue()
	}
}

func (parse *Parser) parseComparisonOperator() (resType token.TokenType, err error) {
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
		val := ast.SQLText(t.Val)
		value = &val
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
		v *= int64(sign)
		val := ast.SQLInt(v)
		return &val, nil
	case token.TT_FLOAT:
		var v float64
		v, err = strconv.ParseFloat(numToken.Val, 64)
		if err != nil {
			err = fmt.Errorf("%v is not a int value", numToken.Val)
			log.Error(err.Error())
			return
		}
		v *= float64(sign)
		val := ast.SQLFloat(v)
		return &val, nil
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
