package bplustree

import (
	"minidb-go/parser/ast"
	p "minidb-go/storage/pager"
	"sort"
)

type DataPage struct {
	size     uint32
	DataList [][]ast.SQLExprValue
}

const order uint32 = 500

var pager = p.GetInstance()

type BPlusTree struct {
	Root      uint32
	FirstLeaf uint32
	LastLeaf  uint32
}

func NewTree(_root uint32, _firstLeaf uint32, _lastLeaf uint32) (tree BPlusTree) {
	tree.Root = _root
	tree.FirstLeaf = _firstLeaf
	tree.LastLeaf = _lastLeaf
	if _root == 0 {
		rootNode := new(BPlusTreeNode)
		rootNode.Addr = pager.NewPage(&rootNode)

		rootNode.Parent = 0
		rootNode.PreLeaf = 0
		rootNode.NextLeaf = 0
		rootNode.Len = 0
		rootNode.isLeaf = true

		tree.Root = rootNode.Addr
		tree.FirstLeaf = rootNode.Addr
		tree.LastLeaf = rootNode.Addr
	}
	return
}

func (tree *BPlusTree) getNode(pageNumber uint32) (node *BPlusTreeNode, err error) {
	page, err := pager.GetPage(pageNumber)
	node = page.(*BPlusTreeNode)
	return
}

func (tree *BPlusTree) searchInTree(key int64) (node *BPlusTreeNode) {
	node, _ = tree.getNode(tree.Root)
	for !node.isLeaf {
		index := sort.Search(len(node.Keys), func(i int) bool { return key < node.Keys[i] })
		node, _ = tree.getNode(node.Values[index])
	}
	return
}

func (tree *BPlusTree) Search(key int64) (row []ast.SQLExprValue) {
	node := tree.searchInTree(key)
	index := sort.Search(node.Len, func(i int) bool { return key == node.Keys[i] })
	if index == node.Len {
		return nil
	}
	pageNumber := node.Values[index]
	if pageNumber == 0 {
		return nil
	}
	dataPage, _ := pager.GetPage(pageNumber)
	data := dataPage.(*DataPage)

	dataIndex := sort.Search(len(data.DataList), func(i int) bool { return data.DataList[i][0] == ast.SQLInt(key) })

	if dataIndex >= len(data.DataList) {
		return nil
	} else {
		row = data.DataList[dataIndex]
	}
	return
}
