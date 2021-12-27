package main

import (
	"bytes"
	"fmt"
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
		Map  map[string]int
	}
	// type P struct {
	// 	X ast.SQLExprValue
	// }
	// var q ast.SQLExprValue
	// p := bplustree.New(1, 2, 3, nil)
	// p := "123"
	// var q = bplustree.BPlusTree{}
	p := P{1, 2, []int32{1, 2, 3, 4, 5}, make(map[string]int)}
	p.Map["1"] = 1
	p.Map["2"] = 2
	q := new(P)
	// p := []int32{1, 2, 3, 4, 5}
	// var q []int32
	util.Encode(buff, p)
	fmt.Println(buff.Bytes())
	util.Decode(buff, q)
	fmt.Println(p, q)

	node := bplustree.BPlusTreeNode{
		Addr:     util.UUID(1),
		Parent:   util.UUID(2),
		PreLeaf:  util.UUID(3),
		NextLeaf: util.UUID(4),
		Len:      5,
		Keys: []bplustree.KeyType{
			util.Uint32ToBytes(4, 1),
			util.Uint32ToBytes(4, 2),
			util.Uint32ToBytes(4, 3),
			util.Uint32ToBytes(4, 4),
			util.Uint32ToBytes(4, 5),
		},
		Values: []bplustree.ValueType{
			util.Uint32ToBytes(4, 1),
			util.Uint32ToBytes(4, 2),
			util.Uint32ToBytes(4, 3),
			util.Uint32ToBytes(4, 4),
			util.Uint32ToBytes(4, 5),
			util.Uint32ToBytes(4, 6),
		},
	}
	raw, _ := node.GobEncode()
	fmt.Println(raw)
	var node2 bplustree.BPlusTreeNode
	tree := bplustree.BPlusTree{
		KeySize:   4,
		ValueSize: 4,
		Order:     5,
	}
	node2.SetTree(&tree)
	node2.SetIsLeaf(false)
	node2.GobDecode(raw)
	fmt.Println(node2)
}
