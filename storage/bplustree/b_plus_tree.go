package bplustree

import (
	"bytes"
	"fmt"
	"minidb-go/storage/index"
	p "minidb-go/storage/pager"
	"minidb-go/util"
	"sync"

	log "github.com/sirupsen/logrus"
)

// b+树，保存其各个关键节点的页号
type BPlusTree struct {
	Root      util.UUID
	FirstLeaf util.UUID
	LastLeaf  util.UUID

	order     uint16
	keySize   uint8
	valueSize uint8

	pager *p.Pager

	lock sync.RWMutex
}

// pager: 分页器
// keySize: 主键的大小， 单位 byte
// valueSize: 值的大小， 单位 byte， 不能小于 4 byte
// return:
// 		tree: b+树
func NewTree(pager *p.Pager, keySize uint8, valueSize uint8) (tree BPlusTree) {
	// order: 每个节点的最大项数，需要为偶数
	order := uint16((util.PAGE_SIZE-1024)/uint16(keySize+valueSize)) / 2 * 2

	rootNode := newNode(order)
	rootPage := pager.NewPage(rootNode)
	rootNode.Addr = rootPage.PageNum()
	rootNode.Parent = 0
	rootNode.PreLeaf = 0
	rootNode.NextLeaf = 0
	rootNode.Len = 0
	rootNode.isLeaf = true

	tree.Root = rootNode.Addr
	tree.FirstLeaf = rootNode.Addr
	tree.LastLeaf = rootNode.Addr
	tree.order = order
	tree.keySize = keySize
	tree.valueSize = valueSize
	tree.pager = pager

	rootNode.Keys = make([]index.KeyType, order)
	rootNode.Values = make([]index.ValueType, order+1)
	return
}

func (tree *BPlusTree) RLock() {
	tree.lock.RLock()
}

func (tree *BPlusTree) RUnlock() {
	tree.lock.RUnlock()
}

func (tree *BPlusTree) Lock() {
	tree.lock.Lock()
}

func (tree *BPlusTree) Unlock() {
	tree.lock.Unlock()
}

func (tree *BPlusTree) Order() uint16 {
	tree.RLock()
	defer tree.RUnlock()

	return tree.order
}

func (tree *BPlusTree) SetOrder(order uint16) {
	tree.Lock()
	defer tree.Unlock()

	tree.order = order
}

func (tree *BPlusTree) KeySize() uint8 {
	tree.RLock()
	defer tree.RUnlock()

	return tree.keySize
}

func (tree *BPlusTree) ValueSize() uint8 {
	tree.RLock()
	defer tree.RUnlock()

	return tree.valueSize
}

func (tree *BPlusTree) SetKeySize(keySize uint8) {
	tree.Lock()
	defer tree.Unlock()

	tree.keySize = keySize
}

func (tree *BPlusTree) SetValueSize(valueSize uint8) {
	tree.Lock()
	defer tree.Unlock()

	tree.valueSize = valueSize
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
	node = &BPlusTreeNode{
		tree: tree,
	}
	_, err = tree.pager.GetPage(pageNum, node)
	return
}

// 返回 key 在 B+树中应该在的第一个位置
// key: 主键
// return:
// 		node: key 应该在的 B+树节点
// 		index: 在节点中的下标
func (tree *BPlusTree) searchLowerInTree(key index.KeyType) (*BPlusTreeNode, uint16) {
	node, err := tree.getNode(tree.Root)
	if err != nil {
		log.Fatalf("tree root page load error: %v", err)
	}
	index := node.LowerBound(key)

	for !node.isLeaf {
		index = node.LowerBound(key)
		node, err = tree.getNode(bytesToUUID(node.Values[index]))
		if err != nil {
			log.Fatal(err)
		}
	}
	return node, uint16(index)
}

func (tree *BPlusTree) searchUpperInTree(key index.KeyType) (*BPlusTreeNode, uint16) {
	node, err := tree.getNode(tree.Root)
	if err != nil {
		log.Fatalf("tree root page load error: %v", err)
	}
	index := node.UpperBound(key)

	for !node.isLeaf {
		index = node.UpperBound(key)
		node, err = tree.getNode(bytesToUUID(node.Values[index]))
		if err != nil {
			log.Fatal(err)
		}
	}
	return node, uint16(index)
}

func (tree *BPlusTree) Search(key index.KeyType) <-chan index.ValueType {
	tree.RLock()
	defer tree.RUnlock()

	valueChan := make(chan index.ValueType, 64)

	node, index := tree.searchLowerInTree(key)
	if uint16(index) == node.Len || compare(node.Keys[index], key) != 0 {
		close(valueChan)
		return valueChan
	}
	nodePageNum := bytesToUUID(node.Values[index])
	if nodePageNum == 0 {
		close(valueChan)
		return valueChan
	}

	// 往 ValueChan 中放入数据
	go func() {
		defer close(valueChan)
		leafNode, err := tree.getNode(nodePageNum)
		if err != nil {
			log.Fatal(err)
		}
		currentIndex := index
		for {
			for currentIndex < node.Len && compare(leafNode.Keys[currentIndex-1], key) == 0 {
				currentValue := leafNode.Values[currentIndex]
				valueChan <- currentValue
				currentIndex++
			}
			// 如果循环到当前 node 的最后一个 Value，则尝试获取下一个 node
			if currentIndex == node.Len {
				// 如果当前 node 是最后一个 node，则退出循环
				if leafNode.NextLeaf == 0 {
					break
				}
				leafNode, err = tree.getNode(leafNode.NextLeaf)
				if err != nil {
					log.Fatal(err)
				}
				currentIndex = 0
			} else {
				break
			}
		}
	}()
	return valueChan
}

// 在 B+树中插入一个 key-value 对，允许有相同的 key
// key: 主键
// value: 值
func (tree *BPlusTree) Insert(key index.KeyType, value index.ValueType) error {
	tree.RLock()
	defer tree.RUnlock()
	// 如果已经存在相同的 (key, value), 则直接返回
	valueChan := tree.Search(key)
	for treeValue := range valueChan {
		if bytes.Equal(treeValue[:], value[:]) {
			return nil
		}
	}
	node, _ := tree.searchLowerInTree(key)

	tree.Lock()
	defer tree.Unlock()
	// TODO: 新插入的 value 需要放在最后一个位置
	ok := node.insertEntry(key, value)
	if !ok {
		err := fmt.Errorf("insert key-value pair failed: key: %v, value: %v", key, value)
		return err
	}
	if node.needSplit() {
		tree.splitLeaf(node)
	}
	return nil
}

func (tree *BPlusTree) splitLeaf(node *BPlusTreeNode) {
	// 如果当前节点是根节点，那需要新建一个根节点作为分裂后节点的父节点
	if node.Addr == tree.Root {
		newRoot := newNode(tree.order)
		rootPage := tree.pager.NewPage(newRoot)
		newRoot.Addr = rootPage.PageNum()
		newRoot.Parent = 0
		newRoot.Len = 0
		newRoot.isLeaf = false
		newRoot.Values[0] = util.UUIDToBytes(tree.valueSize, node.Addr)

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
		tree.FirstLeaf = node.Addr
		tree.LastLeaf = node.Addr
	}

	newNode := newNode(tree.order)
	newNodePage := tree.pager.NewPage(newNode)
	newNode.Addr = newNodePage.PageNum()
	newNode.Parent = node.Parent

	// 更新树的最后一个节点
	if tree.LastLeaf == node.Addr {
		tree.LastLeaf = newNode.Addr
	}

	order := tree.order
	// 复制一半元素
	for i := order / 2; i < node.Len; i++ {
		newNode.Keys[i-order/2] = node.Keys[i]
		newNode.Values[i-order/2] = node.Values[i]
	}
	node.Len = order / 2
	newNode.Len = order - order/2

	newNode.isLeaf = true

	// 重新设置前后节点关系
	newNode.PreLeaf = node.Addr
	newNode.NextLeaf = node.NextLeaf

	// 如果当前节点后面还有节点，还需要更改后一个节点的 preLeaf
	if node.NextLeaf != 0 {
		nextNextLeaf, _ := tree.getNode(node.NextLeaf)
		nextNextLeaf.PreLeaf = newNode.Addr
	}
	node.NextLeaf = newNode.Addr

	if node.Addr == tree.LastLeaf {
		tree.LastLeaf = newNode.Addr
	}

	// 递归更改父节点
	parentNode, _ := tree.getNode(node.Parent)
	parentNode.insertEntry(newNode.Keys[0], util.UUIDToBytes(tree.valueSize, newNode.Addr))
	if parentNode.needSplit() {
		tree.splitParent(parentNode)
	}
}

func (tree *BPlusTree) splitParent(node *BPlusTreeNode) {
	// 如果当前节点是根节点，那需要新建一个根节点作为分裂后节点的父节点
	if node.Addr == tree.Root {
		newRoot := newNode(tree.order)
		newRootPage := tree.pager.NewPage(newRoot)
		newRoot.Addr = newRootPage.PageNum()
		newRoot.Parent = 0
		newRoot.isLeaf = false
		newRoot.Len = 0
		newRoot.Values[0] = util.UUIDToBytes(tree.valueSize, node.Addr)

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
	}

	newNode := newNode(tree.order)
	newNodePage := tree.pager.NewPage(newNode)
	newNode.Addr = newNodePage.PageNum()

	// 复制一半元素
	order := tree.order
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
	for i := uint16(0); i < newNode.Len+1; i++ {
		child, _ := tree.getNode(util.BytesToUUID(newNode.Values[i]))
		child.Parent = newNode.Addr
	}

	k := node.Keys[node.Len]
	v := util.UUIDToBytes(tree.valueSize, newNode.Addr)
	parent, _ := tree.getNode(node.Parent)
	parent.insertEntry(k, v)
	if parent.needSplit() {
		tree.splitParent(parent)
	}
}
