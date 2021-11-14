package bplustree

import "sort"

type BPlusTreeNode struct {
	Addr     uint32
	Parent   uint32
	PreLeaf  uint32
	NextLeaf uint32

	Len    int
	Keys   [order]int64
	Values [order + 1]uint32

	isLeaf bool
}

func (node *BPlusTreeNode) needSplit() bool {
	return node.Len > int(order)-1
}

func (node *BPlusTreeNode) insertEntry(key int64, value uint32) (ok bool) {
	index := sort.Search(node.Len, func(i int) bool { return node.Keys[i] >= key })

	// 如果已经存在 key
	if index < node.Len && node.Keys[index] == key {
		// 被标记删除
		if node.Values[index] == DELETED {
			node.Values[index] = value
			return
		} else {
			// 否则插入失败
			return false
		}
	}

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
