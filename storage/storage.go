package storage

import (
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
)

type metaPage struct {
	Minidb        string
	BPlusTreeList map[string]bplustree.BPlusTree
}

func GetMetaPage() (meta *metaPage) {
	return &metaPage{
		Minidb:        "Minidb",
		BPlusTreeList: make(map[string]bplustree.BPlusTree),
	}
}

func (meta *metaPage) NewTable(tableName string) {
	tree := bplustree.NewTree()
	meta.BPlusTreeList[tableName] = tree
}

func (meta *metaPage) InsertData(tableName string, data bplustree.DataEntry) {
	tree := meta.BPlusTreeList[tableName]
	tree.Insert(data)
}

func (meta *metaPage) UpdateData(tableName string, data bplustree.DataEntry) {
	tree := meta.BPlusTreeList[tableName]
	tree.Update(data)
}

func (meta *metaPage) SearchData(tableName string, key ast.SQLInt) (
	data []ast.SQLExprValue) {
	tree := meta.BPlusTreeList[tableName]
	data = tree.Search(key)
	return
}
