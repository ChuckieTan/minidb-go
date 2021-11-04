package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"minidb-go/pager"
	"minidb-go/parser"
	"minidb-go/util"
	"os"
	"reflect"

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

func dump() {
	type P struct {
		X, Y int64
	}
	v := [253]P{}
	// v := [190]P{}
	for i := 0; i < len(v); i++ {
		v[i].X = math.MaxInt64
		v[i].Y = math.MaxInt64
	}
	defer util.TimeCost()("dump benchmark")
	var d []byte
	for i := 0; i < 100000; i++ {
		d, _ = pager.Dump(v)
		// fmt.Println(len(d))
	}
	fmt.Println(len(d))
}

func gg() {
	type P struct {
		X, Y int64
	}
	v := [253]P{}
	// v := [190]P{}
	for i := 0; i < len(v); i++ {
		v[i].X = math.MaxInt64
		v[i].Y = math.MaxInt64
	}
	defer util.TimeCost()("gob benchmark")
	var network bytes.Buffer
	for i := 0; i < 100000; i++ {
		network = bytes.Buffer{}
		enc := gob.NewEncoder(&network)
		enc.Encode(v)
	}
	fmt.Println(len(network.String()))
}

func P(value interface{}) int {
	return value.(int)
}

func Q(value interface{}) int {
	return int(reflect.ValueOf(value).Int())
}

func PP() {
	defer util.TimeCost()("P benchmark")
	ans := 0
	for i := 1; i < 100000000; i++ {
		ans += P(i)
	}
	fmt.Println(ans)
}
func QQ() {
	defer util.TimeCost()("Q benchmark")
	ans := 0
	for i := 1; i < 100000000; i++ {
		ans += Q(i)
	}
	fmt.Println(ans)
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
	bin, err := pager.Dump(statement)
	fmt.Println(bin, err)

	gg()
	dump()
	// PP()
	// QQ()
}
