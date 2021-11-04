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
	// sql := "select * from student where id = 1;"
	// sql := "update student set id = 1, name = 'tom' where id = 1;"
	sql := "delete from student where id = -1.2;"
	sqlParser, _ := parser.NewParser(sql)
	statement, _ := sqlParser.ParseDeleteStatement()
	fmt.Println(statement)
	// lexer, err := parser.NewLexer(sql)
	// if err == nil {
	// 	t := lexer.GetNextToken()
	// 	fmt.Println(t.Val)
	// 	for t.Type != token.TT_END {
	// 		t = lexer.GetNextToken()
	// 		fmt.Println(t.Val)
	// 	}
	// }
	// parser.BenchMark()
}
