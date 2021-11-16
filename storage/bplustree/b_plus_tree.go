package bplustree

import (
	"minidb-go/parser/ast"
	p "minidb-go/storage/pager"
	"sort"
)

const order uint32 = 500

const DELETED = ^uint32(0)

var pager = p.GetInstance()

type DataEntry struct {
	Key  ast.SQLInt
	Data []ast.SQLExprValue
}

type DataPage struct {
	Addr uint32

	Size     uint32
	DataList []DataEntry

	PreData  uint32
	NextData uint32
}

type BPlusTree struct {
	Root      uint32
	FirstLeaf uint32
	LastLeaf  uint32

	FirstData uint32
	LastData  uint32
}

func NewTree() (tree BPlusTree) {
	rootNode := new(BPlusTreeNode)
	rootNode.Addr = pager.NewPage(rootNode)
	rootNode.Parent = 0
	rootNode.PreLeaf = 0
	rootNode.NextLeaf = 0
	rootNode.Len = 0
	rootNode.isLeaf = true

	dataNode := new(DataPage)
	dataNode.Addr = pager.NewPage(dataNode)
	dataNode.Size = 0
	dataNode.DataList = make([]DataEntry, 0)
	dataNode.PreData = 0
	dataNode.NextData = 0

	tree.Root = rootNode.Addr
	tree.FirstLeaf = rootNode.Addr
	tree.LastLeaf = rootNode.Addr
	tree.FirstData = dataNode.Addr
	tree.LastData = dataNode.Addr
	return
}

func getNode(pageNumber uint32) (node *BPlusTreeNode, err error) {
	page, err := pager.GetPage(pageNumber)
	node = page.(*BPlusTreeNode)
	return
}

func (tree *BPlusTree) searchInTree(key ast.SQLInt) (node *BPlusTreeNode, index int) {
	node, _ = getNode(tree.Root)
	index = sort.Search(node.Len, func(i int) bool { return node.Keys[i] >= key })

	for !node.isLeaf {
		index = sort.Search(node.Len, func(i int) bool { return node.Keys[i] >= key })
		node, _ = getNode(node.Values[index])
	}
	return
}

func (tree *BPlusTree) Search(key ast.SQLInt) (row []ast.SQLExprValue) {
	node, index := tree.searchInTree(key)
	if index == node.Len || node.Keys[index] != key {
		return nil
	}
	pageNumber := node.Values[index]
	if pageNumber == 0 {
		return nil
	}
	rawPage, _ := pager.GetPage(pageNumber)
	dataPage := rawPage.(*DataPage)

	dataIndex := sort.Search(
		len(dataPage.DataList),
		func(i int) bool {
			return dataPage.DataList[i].Key >= key
		},
	)

	if dataIndex >= len(dataPage.DataList) ||
		dataPage.DataList[dataIndex].Key != key {
		return nil
	} else {
		row = dataPage.DataList[dataIndex].Data
	}
	return
}

func (tree *BPlusTree) insertData(row DataEntry) (pageNumber uint32) {
	page, _ := pager.GetPage(tree.LastData)
	dataPage := page.(*DataPage)
	dataPage.DataList = append(dataPage.DataList, row)
	pageNumber = tree.LastData
	return
}

func (tree *BPlusTree) Insert(data DataEntry) (ok bool) {
	ok = true
	node, index := tree.searchInTree(data.Key)
	if index < node.Len && node.Keys[index] == data.Key {
		return false
	}
	pageNumber := tree.insertData(data)
	ok = node.insertEntry(data.Key, pageNumber)
	if !ok {
		return
	}
	if node.needSplit() {
		tree.splitLeaf(node)
	}
	return
}

func (tree *BPlusTree) splitLeaf(node *BPlusTreeNode) {
	// 如果当前节点是根节点，那需要新建一个根节点作为分裂后节点的父节点
	if node.Addr == tree.Root {
		newRoot := new(BPlusTreeNode)
		newRoot.Addr = pager.NewPage(newRoot)
		newRoot.Parent = 0
		newRoot.Len = 0
		newRoot.isLeaf = false
		newRoot.Values[0] = node.Addr

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
		tree.FirstLeaf = node.Addr
		tree.LastLeaf = node.Addr
	}

	newNode := new(BPlusTreeNode)
	newNode.Addr = pager.NewPage(newNode)
	newNode.Parent = node.Parent

	// 复制一半元素
	for i := order / 2; i < uint32(node.Len); i++ {
		newNode.Keys[i-order/2] = node.Keys[i]
		newNode.Values[i-order/2] = node.Values[i]
	}
	node.Len = int(order) / 2
	newNode.Len = int(order) - int(order)/2

	newNode.isLeaf = true

	// 重新设置前后节点关系
	newNode.PreLeaf = node.Addr
	newNode.NextLeaf = node.NextLeaf

	// 如果当前节点后面还有节点，还需要更改后一个节点的 preLeaf
	if node.NextLeaf != 0 {
		nextNextLeaf, _ := getNode(node.NextLeaf)
		nextNextLeaf.PreLeaf = newNode.Addr
	}
	node.NextLeaf = newNode.Addr

	if node.Addr == tree.LastLeaf {
		tree.LastLeaf = newNode.Addr
	}

	// 递归更改父节点
	parentNode, _ := getNode(node.Parent)
	parentNode.insertEntry(newNode.Keys[0], newNode.Addr)
	if parentNode.needSplit() {
		tree.splitParent(parentNode)
	}
}

func (tree *BPlusTree) splitParent(node *BPlusTreeNode) {
	// 如果当前节点是根节点，那需要新建一个根节点作为分裂后节点的父节点
	if node.Addr == tree.Root {
		newRoot := new(BPlusTreeNode)
		newRoot.Addr = pager.NewPage(newRoot)
		newRoot.Parent = 0
		newRoot.isLeaf = false
		newRoot.Len = 0
		newRoot.Values[0] = node.Addr

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
	}

	newNode := new(BPlusTreeNode)
	newNode.Addr = pager.NewPage(newNode)

	// 复制一半元素
	for i := order / 2; i < uint32(node.Len); i++ {
		newNode.Keys[i-order/2] = node.Keys[i]
		newNode.Values[i-order/2] = node.Values[i]
	}
	// 对于非叶子节点，children 比 keys 多一
	newNode.Values[node.Len-int(order)/2] = node.Values[node.Len]
	// node 需要上升一个节点到父节点
	node.Len = int(order)/2 - 1
	newNode.Len = int(order) - int(order)/2

	newNode.isLeaf = false

	// 更新新节点的子节点的父节点
	for i := 0; i < newNode.Len+1; i++ {
		child, _ := getNode(newNode.Values[i])
		child.Parent = newNode.Addr
	}

	k := node.Keys[node.Len]
	v := newNode.Addr
	parent, _ := getNode(node.Parent)
	parent.insertEntry(k, v)
	if parent.needSplit() {
		tree.splitParent(parent)
	}
}

func (tree *BPlusTree) updateData(data DataEntry) (pageNumber uint32, ok bool) {
	rawPage, _ := pager.GetPage(tree.LastData)
	dataPage := rawPage.(*DataPage)

	dataIndex := sort.Search(
		len(dataPage.DataList),
		func(i int) bool {
			return dataPage.DataList[i].Key >= data.Key
		},
	)
	if dataIndex >= len(dataPage.DataList) ||
		dataPage.DataList[dataIndex].Key != data.Key {
		ok = false
		return
	}
	dataPage.DataList[dataIndex] = data
	pageNumber = tree.LastData
	return
}

func (tree *BPlusTree) Update(data DataEntry) (ok bool) {
	ok = true
	node, index := tree.searchInTree(data.Key)
	if index >= node.Len || node.Keys[index] != data.Key {
		return false
	}
	pageNumber, ok := tree.updateData(data)
	node.Values[index] = pageNumber
	return
}
