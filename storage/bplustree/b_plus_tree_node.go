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

	order uint16

	// 只在内存中使用，用于解码
	tree *BPlusTree
	page *pager.Page

	lock sync.RWMutex
}

func newNode(order uint16) *BPlusTreeNode {
	node := new(BPlusTreeNode)
	node.order = order
	node.Keys = make([]index.KeyType, order)
	node.Values = make([]index.ValueType, order+1)
	return node
}

func (node *BPlusTreeNode) UpperBound(key index.KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return compare(node.Keys[i], key) > 0 },
	)
	return uint16(index)
}

func (node *BPlusTreeNode) LowerBound(key index.KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return compare(node.Keys[i], key) >= 0 },
	)
	return uint16(index)
}

func compare(a, b index.KeyType) int {
	return bytes.Compare(a[:], b[:])
}

func (node *BPlusTreeNode) needSplit() bool {
	return node.Len > node.order-1
}

// 可以插入重复的 Key
// TODO: 如果 Key 和 Value 都存在， 则不插入
func (node *BPlusTreeNode) insertEntry(key index.KeyType, value index.ValueType) (ok bool) {
	redolog := redolog.NewBNodeInsertKVLog(
		node.tree.tableId, node.tree.columnId, node.Addr, key, value)
	node.page.AppendLog(redolog)

	index := node.LowerBound(key)

	// 插入 key
	copy(node.Keys[index+1:], node.Keys[index:node.order-1])
	node.Keys[index] = key

	// 插入 value
	if node.isLeaf {
		copy(node.Values[index+1:], node.Values[index:node.order])
		node.Values[index] = value
	} else {
		copy(node.Values[index+2:], node.Values[index+1:node.order])
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
	buff := bytes.NewBuffer(make([]byte, 0))
	intBuff := make([]byte, 4)
	binary.BigEndian.PutUint32(intBuff, uint32(node.Addr))
	buff.Write(intBuff)
	binary.BigEndian.PutUint32(intBuff, uint32(node.Parent))
	buff.Write(intBuff)
	binary.BigEndian.PutUint32(intBuff, uint32(node.PreLeaf))
	buff.Write(intBuff)
	binary.BigEndian.PutUint32(intBuff, uint32(node.NextLeaf))
	buff.Write(intBuff)

	int16Buff := make([]byte, 2)
	binary.BigEndian.PutUint16(int16Buff, node.Len)
	buff.Write(int16Buff)

	// 先编码 isLeaf，因为 Key 和 Value 都是变长的
	if node.isLeaf {
		buff.Write([]byte{1})
	} else {
		buff.Write([]byte{0})
	}
	for i := 0; i < int(node.Len); i++ {
		buff.Write(node.Keys[i][:])
		buff.Write(node.Values[i][:])
	}
	if !node.isLeaf {
		buff.Write(node.Values[node.Len][:])
	}
	return buff.Bytes()
}

func (node *BPlusTreeNode) Decode(r io.Reader) error {
	if node.tree == nil {
		return errors.New("tree is nil")
	}
	intBuff := make([]byte, 4)
	r.Read(intBuff)
	node.Addr = util.UUID(binary.BigEndian.Uint32(intBuff))
	r.Read(intBuff)
	node.Parent = util.UUID(binary.BigEndian.Uint32(intBuff))
	r.Read(intBuff)
	node.PreLeaf = util.UUID(binary.BigEndian.Uint32(intBuff))
	r.Read(intBuff)
	node.NextLeaf = util.UUID(binary.BigEndian.Uint32(intBuff))

	int16Buff := make([]byte, 2)
	r.Read(int16Buff)
	node.Len = binary.BigEndian.Uint16(int16Buff)

	isLeafByte := make([]byte, 1)
	r.Read(isLeafByte)
	if isLeafByte[0] == 1 {
		node.isLeaf = true
	} else {
		node.isLeaf = false
	}
	if node.isLeaf {
		node.Keys = make([]index.KeyType, int(node.tree.order))
		node.Values = make([]index.ValueType, int(node.tree.order))
	} else {
		node.Keys = make([]index.KeyType, int(node.tree.order))
		node.Values = make([]index.ValueType, int(node.tree.order)+1)
	}
	for i := 0; i < int(node.Len); i++ {
		node.Keys[i] = make(index.KeyType, node.tree.KeySize())
		r.Read(node.Keys[i][:])
		node.Values[i] = make(index.ValueType, node.tree.ValueSize())
		r.Read(node.Values[i][:])
	}

	if !node.isLeaf {
		node.Values[node.Len] = make(index.ValueType, node.tree.ValueSize())
		r.Read(node.Values[node.Len][:])
	}
	return nil
}
