package bplustree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"minidb-go/storage/index"
	"minidb-go/storage/pager"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/storage/recovery/redo/redolog"
	"minidb-go/util"
	"sort"
	"sync"
)

type BPlusTreeNode struct {
	Addr     util.UUID
	Parent   util.UUID
	PreLeaf  util.UUID
	NextLeaf util.UUID

	Len    uint16
	Keys   []index.KeyType
	Values []index.ValueType

	isLeaf bool

	// 只在内存中使用，用于解码
	tree *BPlusTree
	page *pager.Page

	lock sync.RWMutex
}

func newNode(tree *BPlusTree) *BPlusTreeNode {
	node := &BPlusTreeNode{
		tree:   tree,
		Keys:   make([]index.KeyType, tree.order),
		Values: make([]index.ValueType, tree.order+1),
	}
	return node
}

func (node *BPlusTreeNode) UpperBound(key index.KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return bytes.Compare(node.Keys[i], key) > 0 },
	)
	return uint16(index)
}

func (node *BPlusTreeNode) LowerBound(key index.KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return bytes.Compare(node.Keys[i], key) >= 0 },
	)
	return uint16(index)
}

func (node *BPlusTreeNode) needSplit() bool {
	return node.Len > node.tree.order-1
}

// 可以插入重复的 Key
// TODO: 如果 Key 和 Value 都存在， 则不插入
func (node *BPlusTreeNode) insertEntry(key index.KeyType, value index.ValueType) (ok bool) {
	redolog := redolog.NewBNodeInsertKVLog(
		node.tree.tableId, node.tree.columnId, node.Addr, key, value)
	node.page.AppendLog(redolog)

	order := node.tree.order
	index := node.LowerBound(key)
	// 插入 key
	copy(node.Keys[index+1:order], node.Keys[index:order-1])
	node.Keys[index] = key

	// 插入 value
	if node.isLeaf {
		copy(node.Values[index+1:], node.Values[index:order])
		node.Values[index] = value
	} else {
		copy(node.Values[index+2:], node.Values[index+1:order])
		node.Values[index+1] = value
	}
	node.Len++
	return
}

func (node *BPlusTreeNode) Size() int {
	var keySize, ValueSize int
	if node.Len == 0 {
		keySize = 0
		ValueSize = 0
	} else {
		keySize = int(node.Len) * len(node.Keys[0])
		ValueSize = int(node.Len+1) * len(node.Values[0])
	}
	return 4 + 4 + 4 + 4 + 2 + keySize + ValueSize + 1 + 2
}

func (node *BPlusTreeNode) PageDataType() pagedata.PageDataType {
	return pagedata.INDEX_DATA
}

func (node *BPlusTreeNode) IsLeaf() bool {
	return node.isLeaf
}

func (node *BPlusTreeNode) SetIsLeaf(isLeaf bool) {
	node.isLeaf = isLeaf
}

func (node *BPlusTreeNode) Tree() *BPlusTree {
	return node.tree
}

func (node *BPlusTreeNode) SetTree(tree *BPlusTree) {
	node.tree = tree
}

func (node *BPlusTreeNode) RLock() {
	node.lock.RLock()
}

func (node *BPlusTreeNode) RUnlock() {
	node.lock.RUnlock()
}

func (node *BPlusTreeNode) Lock() {
	node.lock.Lock()
}

func (node *BPlusTreeNode) Unlock() {
	node.lock.Unlock()
}

func (node *BPlusTreeNode) Encode() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, node.Addr)
	binary.Write(buff, binary.BigEndian, node.Parent)
	binary.Write(buff, binary.BigEndian, node.PreLeaf)
	binary.Write(buff, binary.BigEndian, node.NextLeaf)
	binary.Write(buff, binary.BigEndian, node.Len)

	// 先编码 isLeaf，因为 Key 和 Value 都是变长的
	binary.Write(buff, binary.BigEndian, node.isLeaf)

	for i := 0; i < int(node.Len); i++ {
		buff.Write(node.Keys[i])
		buff.Write(node.Values[i])
	}
	if !node.isLeaf {
		buff.Write(node.Values[node.Len])
	}
	return buff.Bytes()
}

func (node *BPlusTreeNode) Decode(r io.Reader) error {
	if node.tree == nil {
		return errors.New("tree is nil")
	}
	binary.Read(r, binary.BigEndian, &node.Addr)
	binary.Read(r, binary.BigEndian, &node.Parent)
	binary.Read(r, binary.BigEndian, &node.PreLeaf)
	binary.Read(r, binary.BigEndian, &node.NextLeaf)
	binary.Read(r, binary.BigEndian, &node.Len)

	binary.Read(r, binary.BigEndian, &node.isLeaf)

	if node.isLeaf {
		node.Keys = make([]index.KeyType, int(node.tree.order))
		node.Values = make([]index.ValueType, int(node.tree.order))
	} else {
		node.Keys = make([]index.KeyType, int(node.tree.order))
		node.Values = make([]index.ValueType, int(node.tree.order)+1)
	}
	for i := 0; i < int(node.Len); i++ {
		node.Keys[i] = make(index.KeyType, node.tree.KeySize())
		r.Read(node.Keys[i])
		node.Values[i] = make(index.ValueType, node.tree.ValueSize())
		r.Read(node.Values[i])
	}

	if !node.isLeaf {
		node.Values[node.Len] = make(index.ValueType, node.tree.ValueSize())
		r.Read(node.Values[node.Len])
	}
	return nil
}
