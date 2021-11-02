package main

import (
	"fmt"
	"minidb-go/parser"
	"minidb-go/util"
	"os"

	log "github.com/sirupsen/logrus"
)

// 设置 log 输出格式
func init() {
	//设置output,默认为stderr,可以为任何io.Writer，比如文件*os.File
	log.SetOutput(os.Stdout)
	//设置最低loglevel
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(true)
	log.SetFormatter(&util.MyFormatter{})
}

func main() {
	// util.StartUp()
	// sql := util.ReadInput()
	// fmt.Println(sql)
	// sql := "create table student (id int, name text);"
	sql := "insert into student values (1, 'tom');"
	sqlParser, _ := parser.NewParser(&sql)
	fmt.Println(sqlParser.ParseInsertIntoStatement())
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
