package bplustree

import (
	"bytes"
	"minidb-go/storage/pager"
	"minidb-go/util"
	"sort"
)

type BPlusTreeNode struct {
	Addr     util.UUID
	Parent   util.UUID
	PreLeaf  util.UUID
	NextLeaf util.UUID

	Len    uint16
	Keys   []KeyType
	Values []ValueType

	isLeaf bool

	order uint16
}

func newNode(order uint16) *BPlusTreeNode {
	node := new(BPlusTreeNode)
	node.order = order
	node.Keys = make([]KeyType, order)
	node.Values = make([]ValueType, order+1)
	return node
}

func compare(a, b KeyType) int {
	return bytes.Compare(a[:], b[:])
}

func (node *BPlusTreeNode) needSplit() bool {
	return node.Len > node.order-1
}

// 可以插入重复的 Key
// TODO: 如果 Key 和 Value 都存在， 则不插入
func (node *BPlusTreeNode) insertEntry(key KeyType, value ValueType) (ok bool) {
	index := sort.Search(int(node.Len), func(i int) bool { return compare(node.Keys[i], key) >= 0 })

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

func (node *BPlusTreeNode) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 1024))
	util.Encode(buff, node)
	return buff.Bytes()
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
	return 4 + 4 + 4 + 2 + keySize + ValueSize + 1 + 2
}

func (node *BPlusTreeNode) PageDataType() pager.PageDataType {
	return pager.INDEX_DATA
}
