package bplustree

import (
	"bytes"
	"minidb-go/util"
	"sort"
)

type KeyType [util.BPLUSTREE_KEY_LEN]byte
type ValueType int64

type BPlusTreeNode struct {
	Addr     util.UUID
	Parent   util.UUID
	PreLeaf  util.UUID
	NextLeaf util.UUID

	Len    int
	Keys   [order]KeyType
	Values [order + 1]ValueType

	isLeaf bool
}

func compare(a, b KeyType) int {
	return bytes.Compare(a[:], b[:])
}

func (node *BPlusTreeNode) needSplit() bool {
	return node.Len > int(order)-1
}

// 插入数据，key 为主键， Value 为磁盘页号
func (node *BPlusTreeNode) insertEntry(key KeyType, value ValueType) (ok bool) {
	index := sort.Search(node.Len, func(i int) bool { return compare(node.Keys[i], key) >= 0 })

	// 可以插入重复的数据
	// // 如果已经存在 key
	// if index < node.Len && node.Keys[index] == key {
	// 	// 被标记删除
	// 	if node.Values[index] == DELETED {
	// 		node.Values[index] = value
	// 		return
	// 	} else {
	// 		// 否则插入失败
	// 		return false
	// 	}
	// }

	// 插入 key
	copy(node.Keys[index+1:], node.Keys[index:order-1])
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

func (node *BPlusTreeNode) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, 1024))
	util.Encode(buff, node)
	return buff.Bytes()
}
