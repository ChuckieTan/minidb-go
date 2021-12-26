package bplustree

import (
	"bytes"
	p "minidb-go/storage/pager"
	"minidb-go/util"
	"sort"

	log "github.com/sirupsen/logrus"
)

type KeyType []byte

// Value 的数据类型， 不能小于 32 位 (4 byte)
type ValueType []byte

// b+树，保存其各个关键节点的页号
type BPlusTree struct {
	Root      util.UUID
	FirstLeaf util.UUID
	LastLeaf  util.UUID

	Order     uint16
	KeySize   uint8
	ValueSize uint8

	pager *p.Pager
}

// 新建一个 b+树
func NewTree(pager *p.Pager, KeySize uint8, ValueSize uint8) (tree BPlusTree) {
	order := uint16((util.PageSize - 1024) / uint16(KeySize+ValueSize))

	rootNode := newNode(order)
	rootPage := pager.NewPage(rootNode, 0)
	rootNode.Addr = rootPage.PageNum()
	rootNode.Parent = 0
	rootNode.PreLeaf = 0
	rootNode.NextLeaf = 0
	rootNode.Len = 0
	rootNode.isLeaf = true

	tree.Root = rootNode.Addr
	tree.FirstLeaf = rootNode.Addr
	tree.LastLeaf = rootNode.Addr
	tree.Order = order
	tree.KeySize = KeySize
	tree.ValueSize = ValueSize

	rootNode.Keys = make([]KeyType, order)
	rootNode.Values = make([]ValueType, order+1)
	return
}

func bytesToUUID(bytes []byte) util.UUID {
	if len(bytes) != 4 {
		return 0
	}
	return util.UUID(util.BytesToUInt32(bytes))
}

// 获取页号对应的 b+树节点
// pageNumber: 页号
// return:
// 		node: B+树节点
func (tree *BPlusTree) getNode(pageNum util.UUID) (node *BPlusTreeNode, err error) {
	page, err := tree.pager.GetPage(pageNum)
	node = (*page.Data()).(*BPlusTreeNode)
	return
}

// 返回 key 在 B+树中应该在的第一个位置
// key: 主键
// return:
// 		node: key 应该在的 B+树节点
// 		index: 在节点中的下标
func (tree *BPlusTree) searchInTree(key KeyType) (*BPlusTreeNode, uint16) {
	node, err := tree.getNode(tree.Root)
	if err != nil {
		log.Fatalf("tree root page load error: %v", err)
	}
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return compare(node.Keys[i], key) >= 0 },
	)

	for !node.isLeaf {
		index = sort.Search(
			int(node.Len),
			func(i int) bool { return compare(node.Keys[i], key) >= 0 },
		)
		node, err = tree.getNode(bytesToUUID(node.Values[index]))
		if err != nil {
			log.Fatal(err)
		}
	}
	return node, uint16(index)
}

func (tree *BPlusTree) Search(key KeyType) <-chan ValueType {
	valueChan := make(chan ValueType)

	node, index := tree.searchInTree(key)
	if uint16(index) == node.Len || compare(node.Keys[index], key) != 0 {
		close(valueChan)
		return valueChan
	}
	pageNum := bytesToUUID(node.Values[index])
	if pageNum == 0 {
		close(valueChan)
		return valueChan
	}

	// 往 ValueChan 中放入数据
	go func() {
		nodePage, err := tree.pager.GetPage(pageNum)
		if err != nil {
			log.Fatal(err)
		}
		leafNode := (*nodePage.Data()).(*BPlusTreeNode)
		for {
			currentIndex := index
			// 先往管道中放入一个 Value，用于去重
			// 如果下一个 Value 和当前 Value 相同，则不放入管道
			preValue := leafNode.Values[currentIndex]
			valueChan <- preValue
			currentIndex++
			for currentIndex < node.Len && compare(leafNode.Keys[currentIndex-1], key) == 0 {
				currentValue := leafNode.Values[currentIndex]
				if bytes.Compare(preValue[:], currentValue) != 0 {
					valueChan <- currentValue
					preValue = currentValue
				}
				currentIndex++
			}
			// 如果循环到当前 node 的最后一个 Value，则尝试获取下一个 node
			if currentIndex == node.Len {
				// 如果当前 node 是最后一个 node，则退出循环
				if leafNode.NextLeaf == 0 {
					break
				}
				nodePage, err = tree.pager.GetPage(leafNode.NextLeaf)
				if err != nil {
					log.Fatal(err)
				}
				leafNode = (*nodePage.Data()).(*BPlusTreeNode)
				currentIndex = 0
			} else {
				break
			}
		}
		close(valueChan)
	}()
	return valueChan
}

func (tree *BPlusTree) Insert(key KeyType, value ValueType) (ok bool) {
	ok = true
	node, index := tree.searchInTree(key)
	if index < node.Len && compare(node.Keys[index], key) == 0 {
		return false
	}
	// TODO: 新插入的 value 需要放在最后一个位置
	ok = node.insertEntry(key, value)
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
		newRoot := newNode(tree.Order)
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

	newNode := newNode(tree.Order)
	newNode.Addr = pager.NewPage(newNode)
	newNode.Parent = node.Parent

	// 复制一半元素
	for i := order / 2; i < node.Len; i++ {
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
		newRoot := newNode(tree.Order)
		newRoot.Addr = pager.NewPage(newRoot)
		newRoot.Parent = 0
		newRoot.isLeaf = false
		newRoot.Len = 0
		newRoot.Values[0] = node.Addr

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
	}

	newNode := newNode(tree.Order)
	newNode.Addr = pager.NewPage(newNode)

	// 复制一半元素
	order := tree.Order
	for i := order / 2; i < node.Len; i++ {
		newNode.Keys[i-order/2] = node.Keys[i]
		newNode.Values[i-order/2] = node.Values[i]
	}
	// 对于非叶子节点，children 比 keys 多一
	newNode.Values[node.Len-order/2] = node.Values[node.Len]
	// node 需要上升一个节点到父节点
	node.Len = order/2 - 1
	newNode.Len = order - order/2

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

func (tree *BPlusTree) updateData(data DataEntry) (pageNumber util.UUID, ok bool) {
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
	// 删除对应的 data
	dataPage.DataList = append(dataPage.DataList[:dataIndex], dataPage.DataList[dataIndex+1:]...)
	pageNumber = tree.insertData(data)
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
