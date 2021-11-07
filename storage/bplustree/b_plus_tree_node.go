package bplustree

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
