package parser_test

import (
	"minidb-go/parser"
	"testing"
)

func BenchmarkParser(b *testing.B) {
	sqls := []string{
		"create table student (id int, name text);",
		"select * from student where id = 1;",
		"insert into student values (1, 'tom');",
		"update student set id = 1, name = 'tom' where id = 1;",
		"delete from student where id = 1;",
	}
	for i := 0; i < b.N; i++ {
		sqlParser, _ := parser.NewParser(sqls[i%len(sqls)])
		sqlParser.ParseStatement()
	}
}
