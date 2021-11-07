package storage

import "minidb-go/storage/bplustree"

type MetaPage struct {
	Minidb        string
	BPlusTreeList map[string]bplustree.BPlusTree
}
