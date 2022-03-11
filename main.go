package main

import (
	"encoding/gob"
	"fmt"
	"minidb-go/parser"
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
	"minidb-go/tbm"
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

	// 注册 gob 接口类型
	gob.Register(bplustree.BPlusTree{})
	gob.Register(ast.SQLInt(0))
	gob.Register(ast.SQLFloat(0))
	gob.Register(ast.SQLText(""))
	gob.Register(ast.SQLColumn(""))
}

func main() {
	sql0 := "create table student (id int, name text);"
	sql1 := "insert into student values (1, 'tom');"
	sql2 := "select * from student where id = 1;"
	sql3 := "update student set id = 1, name = 'sam' where id = 1;"

	// tbm := tbm.Create("/tmp/test/")
	tbm := tbm.Open("/tmp/test/")
	xid := tbm.Begin()
	p, _ := parser.NewParser(sql0)
	stmt, _ := p.ParseStatement()
	createStmt := stmt.(ast.CreateTableStmt)
	err := tbm.CreateTable(xid, createStmt)
	fmt.Println(err)

	p, _ = parser.NewParser(sql1)
	stmt, _ = p.ParseStatement()
	insertStmt := stmt.(ast.InsertIntoStmt)
	result, err := tbm.Insert(xid, insertStmt)
	fmt.Println(result, err)

	p, _ = parser.NewParser(sql2)
	stmt, _ = p.ParseStatement()
	selectStmt := stmt.(ast.SelectStmt)
	result, err = tbm.Select(xid, selectStmt)
	fmt.Println(result, err)

	p, _ = parser.NewParser(sql3)
	stmt, _ = p.ParseStatement()
	updateStmt := stmt.(ast.UpdateStmt)
	result, err = tbm.Update(xid, updateStmt)
	fmt.Println(result, err)

	p, _ = parser.NewParser(sql2)
	stmt, _ = p.ParseStatement()
	selectStmt = stmt.(ast.SelectStmt)
	result, err = tbm.Select(xid, selectStmt)
	fmt.Println(result, err)

	tbm.Commit(xid)

	tbm.Close()
}
