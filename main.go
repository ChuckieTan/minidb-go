package main

import (
	"bytes"
	"fmt"
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
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

	buff := new(bytes.Buffer)
	type P struct {
		X, Y int32
		Arr  []int32
	}
	// type P struct {
	// 	X ast.SQLExprValue
	// }
	// var q ast.SQLExprValue
	// p := bplustree.New(1, 2, 3, nil)
	// p := "123"
	// var q = bplustree.BPlusTree{}
	p := P{1, 2, []int32{1, 2, 3, 4, 5}}
	q := new(P)
	// p := []int32{1, 2, 3, 4, 5}
	// var q []int32
	util.Encode(buff, p)
	fmt.Println(buff.Bytes())
	util.Decode(buff, q)
	fmt.Println(p, q)

	tree := bplustree.NewTree()
	tree.Insert(1, []ast.SQLExprValue{ast.SQLInt(1), ast.SQLInt(200)})
	fmt.Println(tree.Search(1))

	// sql := "delete from student where id = -1.2;"
	// sqlParser, _ := parser.NewParser(sql)
	// statement, _ := sqlParser.ParseDeleteStatement()
	// fmt.Println(statement)
	// d := ast.DeleteStatement{}

	// sql := "create table student (id int, name text);"
	// sqlParser, _ := parser.NewParser(sql)
	// statement, _ := sqlParser.ParseCreateTableStatement()
	// fmt.Println(statement)
	// d := ast.CreateTableStatement{}

	statement := ast.ColumnType(1)
	d := ast.ColumnType(2)
	util.Encode(buff, statement)
	fmt.Println(buff.Bytes())
	util.Decode(buff, &d)
	// fmt.Println(d)
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
