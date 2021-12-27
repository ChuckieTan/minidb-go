package bplustree

import (
	"bytes"
	"encoding/binary"
	"errors"
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

	// 只在内存中使用，用于解码
	tree *BPlusTree
}

func newNode(order uint16) *BPlusTreeNode {
	node := new(BPlusTreeNode)
	node.order = order
	node.Keys = make([]KeyType, order)
	node.Values = make([]ValueType, order+1)
	return node
}

func (node *BPlusTreeNode) UpperBound(key KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return compare(node.Keys[i], key) > 0 },
	)
	return uint16(index)
}

func (node *BPlusTreeNode) LowerBound(key KeyType) uint16 {
	index := sort.Search(
		int(node.Len),
		func(i int) bool { return compare(node.Keys[i], key) >= 0 },
	)
	return uint16(index)
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
	return 4 + 4 + 4 + 2 + keySize + ValueSize + 1 + 2
}

func (node *BPlusTreeNode) PageDataType() pager.PageDataType {
	return pager.INDEX_DATA
}

func (node *BPlusTreeNode) IsLeaf() bool {
	return node.isLeaf
}

func (node *BPlusTreeNode) SetIsLeaf(isLeaf bool) {
	node.isLeaf = isLeaf
}

func (node *BPlusTreeNode) GobEncode() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 1024))
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
	return buff.Bytes(), nil
}

func (node *BPlusTreeNode) Tree() *BPlusTree {
	return node.tree
}

func (node *BPlusTreeNode) SetTree(tree *BPlusTree) {
	node.tree = tree
}

func (node *BPlusTreeNode) GobDecode(data []byte) error {
	if node.tree == nil {
		return errors.New("tree is nil")
	}
	buff := bytes.NewBuffer(data)
	intBuff := make([]byte, 4)
	buff.Read(intBuff)
	node.Addr = util.UUID(binary.BigEndian.Uint32(intBuff))
	buff.Read(intBuff)
	node.Parent = util.UUID(binary.BigEndian.Uint32(intBuff))
	buff.Read(intBuff)
	node.PreLeaf = util.UUID(binary.BigEndian.Uint32(intBuff))
	buff.Read(intBuff)
	node.NextLeaf = util.UUID(binary.BigEndian.Uint32(intBuff))

	int16Buff := make([]byte, 2)
	buff.Read(int16Buff)
	node.Len = binary.BigEndian.Uint16(int16Buff)

	isLeafByte, _ := buff.ReadByte()
	if isLeafByte == 1 {
		node.isLeaf = true
	} else {
		node.isLeaf = false
	}
	if node.isLeaf {
		node.Keys = make([]KeyType, node.tree.Order)
		node.Values = make([]ValueType, node.tree.Order+1)
	} else {
		node.Keys = make([]KeyType, node.tree.Order)
		node.Values = make([]ValueType, node.tree.Order)
	}
	for i := 0; i < int(node.Len); i++ {
		node.Keys[i] = make(KeyType, node.tree.KeySize)
		buff.Read(node.Keys[i][:])
		node.Values[i] = make(ValueType, node.tree.ValueSize)
		buff.Read(node.Values[i][:])
	}

	if !node.isLeaf {
		node.Values[node.Len] = make(ValueType, node.tree.ValueSize)
		buff.Read(node.Values[node.Len][:])
	}
	return nil
}
