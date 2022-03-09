package bplustree

import (
	"minidb-go/storage/pager"
	"minidb-go/storage/recovery/redo/redolog"
	"minidb-go/util"
)

func (tree *BPlusTree) RecoverInsertKV(log *redolog.BNodeInsertKVLog) error {
	key := log.Key()
	value := log.Value()
	page, node, err := tree.getNodePage(log.PageNum())
	if err != nil {
		return err
	}
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

	tree.pager.Flush(page)
	return nil
}

func (tree *BPlusTree) getNodePage(pageNum util.UUID) (*pager.Page, *BPlusTreeNode, error) {
	node := &BPlusTreeNode{
		tree: tree,
	}
	page, err := tree.pager.GetPage(pageNum, node)
	return page, node, err
}

func (tree *BPlusTree) RecoverSplitNode(log *redolog.BNodeSplitLog) error {
	page, node, err := tree.getNodePage(log.PageNum())
	if err != nil {
		return err
	}
	nextPage, _, err := tree.getNodePage(log.NextPageNum())
	if err != nil {
		return err
	}
	if node.isLeaf {
		err = tree.recoverSplitLeafNode(page, nextPage, log)
	} else {
		err = tree.recoverSplitNonLeafNode(page, nextPage, log)
	}
	tree.pager.Flush(page)
	tree.pager.Flush(nextPage)
	return err
}

func (tree *BPlusTree) recoverSplitLeafNode(page *pager.Page, nextPage *pager.Page,
	log *redolog.BNodeSplitLog) error {
	node := page.Data().(*BPlusTreeNode)
	nextNode := nextPage.Data().(*BPlusTreeNode)
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

	nextNode.Addr = nextPage.PageNum()
	nextNode.Parent = node.Parent

	// 更新树的最后一个节点
	if tree.LastLeaf == node.Addr {
		tree.LastLeaf = nextNode.Addr
	}

	order := tree.order
	// 复制一半元素
	for i := order / 2; i < node.Len; i++ {
		nextNode.Keys[i-order/2] = node.Keys[i]
		nextNode.Values[i-order/2] = node.Values[i]
	}
	node.Len = order / 2
	nextNode.Len = order - order/2

	nextNode.isLeaf = true

	// 重新设置前后节点关系
	nextNode.PreLeaf = node.Addr
	nextNode.NextLeaf = node.NextLeaf

	// 如果当前节点后面还有节点，还需要更改后一个节点的 preLeaf
	if node.NextLeaf != 0 {
		nextNextLeaf, _ := tree.getNode(node.NextLeaf)
		nextNextLeaf.PreLeaf = nextNode.Addr
	}
	node.NextLeaf = nextNode.Addr

	if node.Addr == tree.LastLeaf {
		tree.LastLeaf = nextNode.Addr
	}

	// 不需要递归更改父节点
	// // 递归更改父节点
	// parentNode, _ := tree.getNode(node.Parent)
	// parentNode.insertEntry(newNode.Keys[0], util.UUIDToBytes(tree.valueSize, newNode.Addr))
	// if parentNode.needSplit() {
	// 	tree.splitParent(parentNode)
	// }
	return nil
}

func (tree *BPlusTree) recoverSplitNonLeafNode(page *pager.Page, nextPage *pager.Page,
	log *redolog.BNodeSplitLog) error {
	node := page.Data().(*BPlusTreeNode)
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

	nextNode := nextPage.Data().(*BPlusTreeNode)
	nextNode.Addr = nextPage.PageNum()

	// 复制一半元素
	order := tree.order
	for i := order / 2; i < node.Len; i++ {
		nextNode.Keys[i-order/2] = node.Keys[i]
		nextNode.Values[i-order/2] = node.Values[i]
	}
	// 对于非叶子节点，children 比 keys 多一
	nextNode.Values[node.Len-order/2] = node.Values[node.Len]
	// node 需要上升一个节点到父节点
	node.Len = order/2 - 1
	nextNode.Len = order - order/2

	nextNode.isLeaf = false

	// 更新新节点的子节点的父节点
	for i := uint16(0); i < nextNode.Len+1; i++ {
		child, _ := tree.getNode(util.BytesToUUID(nextNode.Values[i]))
		child.Parent = nextNode.Addr
	}

	// 不用递归更改父节点
	// k := node.Keys[node.Len]
	// v := util.UUIDToBytes(tree.valueSize, newNode.Addr)
	// parent, _ := tree.getNode(node.Parent)
	// parent.insertEntry(k, v)
	// if parent.needSplit() {
	// 	tree.splitParent(parent)
	// }
	return nil
}
