package main

import (
	"fmt"
	"minidb-go/parser"
)

func main() {
	// util.StartUp()
	// sql := util.ReadInput()
	// fmt.Println(sql)
	sql := "select sum(a) from student;"
	lexer, err := parser.NewLexer(sql)
	if err == nil {
		token := lexer.GetNextToken()
		fmt.Println(token.Val)
		for token.Type != parser.TT_END {
			token = lexer.GetNextToken()
			fmt.Println(token.Val)
		}
	}
}
