package tbm_test

import (
	"fmt"
	"minidb-go/parser"
	"minidb-go/parser/ast"
	"minidb-go/tbm"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	f, e := os.MkdirTemp("/tmp/test/", "")
	if e != nil {
		return ""
	}
	return f
}

func BenchmarkInsert(b *testing.B) {
	path := createtmpdir()
	tbm := tbm.Create(path)
	stmt, _ := parser.Parse("create table t1(id int, name text, age int);")
	createStmt := stmt.(ast.CreateTableStmt)
	tbm.CreateTable(0, createStmt)
	stmts := make([]ast.InsertIntoStmt, b.N)
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("insert into t1 values(%d, '%s', %d);", i, "test", i)
		stmt, _ := parser.Parse(stmtStr)
		stmts[i] = stmt.(ast.InsertIntoStmt)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbm.Insert(1, stmts[i])
	}
	destorytemp(path)
}

func BenchmarkSelect(b *testing.B) {
	path := createtmpdir()
	tbm := tbm.Create(path)
	stmt, _ := parser.Parse("create table t1(id int, name text, age int);")
	createStmt := stmt.(ast.CreateTableStmt)
	tbm.CreateTable(0, createStmt)
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("insert into t1 values(%d, '%s', %d);", i, "test", i)
		stmt, _ := parser.Parse(stmtStr)
		insertStmt := stmt.(ast.InsertIntoStmt)
		tbm.Insert(1, insertStmt)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmtStr := fmt.Sprintf("select * from t1 where id = %v;", i)
		stmt, _ := parser.Parse(stmtStr)
		selectStmt := stmt.(ast.SelectStmt)
		tbm.Select(1, selectStmt)
	}
	destorytemp(path)
}
