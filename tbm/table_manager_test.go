package tbm_test

import (
	"encoding/gob"
	"fmt"
	"minidb-go/parser"
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
	"minidb-go/tbm"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func init() {
	gob.Register(bplustree.BPlusTree{})
	gob.Register(ast.SQLInt(0))
	gob.Register(ast.SQLFloat(0))
	gob.Register(ast.SQLText(""))
	gob.Register(ast.SQLColumn(""))
}

func destorytemp(path string) {
	filepath.Walk(path, func(path string, fi os.FileInfo, err error) error {
		if nil == fi {
			return err
		}
		if !fi.IsDir() {
			return nil
		}
		name := fi.Name()

		if strings.Contains(name, "temp") {

			fmt.Println("temp file name:", path)

			err := os.RemoveAll(path)
			if err != nil {
				fmt.Println("delet dir error:", err)
			}
		}
		return nil
	})

}

func createtmpdir() string {
	dir, e := os.MkdirTemp("", "")
	// dir, e := os.MkdirTemp("/tmp/test/", "")
	if e != nil {
		return ""
	}
	return dir
}

func BenchmarkInsert(b *testing.B) {
	path := createtmpdir()
	logrus.Info(path)
	tbm := tbm.Create(path)
	stmt, _ := parser.Parse("create table t1(id int, name text, age int);")
	createStmt := stmt.(ast.CreateTableStmt)
	xid := tbm.Begin()
	tbm.CreateTable(xid, createStmt)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("insert into t1 values(%d, '%s', %d);", i, "test student insert", i)
		stmt, _ := parser.Parse(stmtStr)
		tbm.Insert(xid, stmt.(ast.InsertIntoStmt))
	}
	tbm.Commit(xid)
	tbm.Close()
	destorytemp(path)
}

func BenchmarkSelect(b *testing.B) {
	path := createtmpdir()
	tbm := tbm.Create(path)
	stmt, _ := parser.Parse("create table t1(id int, name text, age int);")
	createStmt := stmt.(ast.CreateTableStmt)
	xid := tbm.Begin()
	tbm.CreateTable(xid, createStmt)
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("insert into t1 values(%d, '%s', %d);", i, "test", i)
		stmt, _ := parser.Parse(stmtStr)
		insertStmt := stmt.(ast.InsertIntoStmt)
		tbm.Insert(xid, insertStmt)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("select * from t1 where id = %v;", i)
		stmt, _ := parser.Parse(stmtStr)
		selectStmt := stmt.(ast.SelectStmt)
		_, err := tbm.Select(xid, selectStmt)
		if err != nil {
			logrus.Error(err)
		}
	}
	destorytemp(path)
}
