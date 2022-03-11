/*
可并行访问的 B+树，只能查找和插入，不能删除和修改
B+ 树并行访问协议如下：
对于查询操作，从根节点开始，首先获取根节点的读锁，
然后在根节点中查找key应该出现的孩子节点，获取孩子节点的读锁，
然后释放根节点的读锁，以此类推，直到找到目标叶子节点，此时该叶子节点获取了读锁。

对于删除和插入操作，也是从根节点开始，
先获取根节点的写锁，一旦孩子节点也获取了写锁，
检查根节点是否安全，如果安全释放孩子节点所有祖先节点的写锁，
以此类推，直到找到目标叶子节点。
节点安全定义如下：如果对于插入操作，如果再插入一个元素，不会产生分裂，
或者对于删除操作，如果再删除一个元素，不会产生并合。
*/
package bplustree

import (
	"bytes"
	"fmt"
	"minidb-go/storage/index"
	p "minidb-go/storage/pager"
	"minidb-go/storage/recovery"
	"minidb-go/storage/recovery/redo/redolog"
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

	tableId  uint16
	columnId uint16

	pager *p.Pager
	rec   *recovery.Recovery

	lock sync.RWMutex
}

// pager: 分页器
// keySize: 主键的大小， 单位 byte
// valueSize: 值的大小， 单位 byte， 不能小于 4 byte
// return:
// 		tree: b+树
func NewTree(pager *p.Pager, keySize uint8, valueSize uint8,
	tableId uint16, columnId uint16, rec *recovery.Recovery) *BPlusTree {
	// order: 每个节点的最大项数，需要为偶数
	order := uint16((util.PAGE_SIZE-1024)/uint16(keySize+valueSize)) / 2 * 2

	rootNode := &BPlusTreeNode{
		Keys:   make([]index.KeyType, order),
		Values: make([]index.ValueType, order+1),
	}
	rootPage := pager.NewPage(rootNode)
	rootNode.page = rootPage
	rootNode.Addr = rootPage.PageNum()
	rootNode.Parent = 0
	rootNode.PreLeaf = 0
	rootNode.NextLeaf = 0
	rootNode.Len = 0
	rootNode.isLeaf = true

	tree := new(BPlusTree)
	tree.Root = rootNode.Addr
	tree.FirstLeaf = rootNode.Addr
	tree.LastLeaf = rootNode.Addr
	tree.order = order
	tree.keySize = keySize
	tree.valueSize = valueSize
	tree.pager = pager
	tree.tableId = tableId
	tree.columnId = columnId
	tree.rec = rec

	rootNode.Keys = make([]index.KeyType, order)
	rootNode.Values = make([]index.ValueType, order+1)
	rootNode.tree = tree

	tree.pager.Flush(rootPage)
	return tree
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

func (tree *BPlusTree) TableId() uint16 {
	return tree.tableId
}

func (tree *BPlusTree) ColumnId() uint16 {
	return tree.columnId
}

func (tree *BPlusTree) Order() uint16 {
	tree.RLock()
	defer tree.RUnlock()

	return tree.order
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
func (tree *BPlusTree) getNode(pageNum util.UUID) (*BPlusTreeNode, error) {
	node := &BPlusTreeNode{
		tree: tree,
	}
	page, err := tree.pager.GetPage(pageNum, node)
	node = page.Data().(*BPlusTreeNode)
	node.page = page
	node.tree = tree
	return node, err
}

type VisitType bool

const (
	Visit_Read  VisitType = true
	Visit_Write VisitType = false
)

func lockNode(node *BPlusTreeNode, visit VisitType) {
	if visit == Visit_Read {
		node.RLock()
	} else {
		node.Lock()
	}
}

func unlockNode(node *BPlusTreeNode, visit VisitType) {
	if visit == Visit_Read {
		node.RUnlock()
	} else {
		node.Unlock()
	}
}

// 返回 key 在 B+树中应该在的第一个位置
// key: 主键
// return:
// 		node: key 应该在的 B+树节点
// 		index: 在节点中的下标
func (tree *BPlusTree) searchLowerInTree(key index.KeyType, visit VisitType) (*BPlusTreeNode, uint16) {
	node, err := tree.getNode(tree.Root)
	// log.Info(node.Addr)
	if err != nil {
		log.Fatalf("tree root page load error: %v", err)
	}
	lockNode(node, visit)
	index := node.LowerBound(key)

	for !node.isLeaf {
		if node.Len == 0 {
			break
		}
		index = node.LowerBound(key)
		childNode, err := tree.getNode(bytesToUUID(node.Values[index]))
		if err != nil {
			log.Fatal(err)
		}
		lockNode(childNode, visit)
		unlockNode(node, visit)
		node = childNode
	}
	return node, uint16(index)
}

func (tree *BPlusTree) searchUpperInTree(key index.KeyType, visit VisitType) (*BPlusTreeNode, uint16) {
	node, err := tree.getNode(tree.Root)
	if err != nil {
		log.Fatalf("tree root page load error: %v", err)
	}
	lockNode(node, visit)
	index := node.UpperBound(key)

	for !node.isLeaf {
		index = node.UpperBound(key)
		childnNode, err := tree.getNode(bytesToUUID(node.Values[index]))
		if err != nil {
			log.Fatal(err)
		}
		lockNode(childnNode, visit)
		unlockNode(node, visit)
	}
	return node, uint16(index)
}

func (tree *BPlusTree) Search(key index.KeyType) <-chan index.ValueType {
	valueChan := make(chan index.ValueType, 64)

	leafNode, index := tree.searchLowerInTree(key, true)
	if uint16(index) == leafNode.Len || compare(leafNode.Keys[index], key) != 0 {
		close(valueChan)
		unlockNode(leafNode, Visit_Read)
		return valueChan
	}

	// 往 ValueChan 中放入数据
	go func() {
		defer close(valueChan)
		currentIndex := index
		for {
			for currentIndex < leafNode.Len && compare(leafNode.Keys[currentIndex-1], key) == 0 {
				currentValue := leafNode.Values[currentIndex]
				valueChan <- currentValue
				currentIndex++
			}
			// 如果循环到当前 node 的最后一个 Value，则尝试获取下一个 node
			if currentIndex == leafNode.Len {
				// 如果当前 node 是最后一个 node，则退出循环
				if leafNode.NextLeaf == 0 {
					break
				}
				nextLeafNode, err := tree.getNode(leafNode.NextLeaf)
				lockNode(nextLeafNode, Visit_Read)
				unlockNode(leafNode, Visit_Read)
				leafNode = nextLeafNode
				if err != nil {
					log.Fatal(err)
				}
				currentIndex = 0
			} else {
				break
			}
		}
		unlockNode(leafNode, Visit_Read)
	}()
	return valueChan
}

// 在 B+树中插入一个 key-value 对，允许有相同的 key
// key: 主键
// value: 值
func (tree *BPlusTree) Insert(key index.KeyType, value index.ValueType) error {
	// 如果已经存在相同的 (key, value), 则直接返回
	valueChan := tree.Search(key)
	for treeValue := range valueChan {
		if bytes.Equal(treeValue[:], value[:]) {
			return nil
		}
	}
	node, _ := tree.searchLowerInTree(key, Visit_Write)
	defer unlockNode(node, Visit_Write)

	// TODO: 新插入的 value 需要放在最后一个位置
	ok := node.insertEntry(key, value)
	tree.rec.Write(node.page)

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
	lockNode(node, Visit_Write)
	defer unlockNode(node, Visit_Write)

	if node.Addr == tree.Root {
		newRoot := newNode(tree)
		rootPage := tree.pager.NewPage(newRoot)
		newRoot.page = rootPage
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

	newNode := newNode(tree)
	newNodePage := tree.pager.NewPage(newNode)
	newNode.page = newNodePage
	newNode.Addr = newNodePage.PageNum()
	newNode.Parent = node.Parent

	redolog := redolog.NewBNodeSplitLog(
		tree.tableId, tree.columnId, node.Addr, newNode.Addr)
	node.page.AppendLog(redolog)

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

	tree.rec.Write(node.page)
	tree.rec.Write(newNode.page)

	// 递归更改父节点
	parentNode, _ := tree.getNode(node.Parent)
	parentNode.insertEntry(newNode.Keys[0], util.UUIDToBytes(tree.valueSize, newNode.Addr))
	if parentNode.needSplit() {
		tree.splitParent(parentNode)
	}
}

func (tree *BPlusTree) splitParent(node *BPlusTreeNode) {
	lockNode(node, Visit_Write)
	defer unlockNode(node, Visit_Write)
	// 如果当前节点是根节点，那需要新建一个根节点作为分裂后节点的父节点
	if node.Addr == tree.Root {
		newRoot := newNode(tree)
		newRootPage := tree.pager.NewPage(newRoot)
		newRoot.page = newRootPage
		newRoot.Addr = newRootPage.PageNum()
		newRoot.Parent = 0
		newRoot.isLeaf = false
		newRoot.Len = 0
		newRoot.Values[0] = util.UUIDToBytes(tree.valueSize, node.Addr)

		node.Parent = newRoot.Addr

		tree.Root = newRoot.Addr
	}

	newNode := newNode(tree)
	newNodePage := tree.pager.NewPage(newNode)
	newNode.page = newNodePage
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

		tree.rec.Write(child.page)
	}

	// 写入 page 到 double write
	tree.rec.Write(node.page)
	tree.rec.Write(newNode.page)

	k := node.Keys[node.Len]
	v := util.UUIDToBytes(tree.valueSize, newNode.Addr)
	parent, _ := tree.getNode(node.Parent)
	parent.insertEntry(k, v)
	if parent.needSplit() {
		tree.splitParent(parent)
	}
}
