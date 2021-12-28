/*
DataManager 负责管理数据文件，提供数据和索引的读写操作
DataManager 中的数据没有版本号的概念，每次选择出的数据都是所有版本的数据
DataManager 中的数据具有持久性，在异常退出后会自动进行恢复
	该特性是由 Redo Log 和 double write 共同支持的
*/
package storage

import (
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
	"minidb-go/storage/pager"
)

type DataManager struct {
	pager *pager.Pager
}

func Create(path string) *DataManager {
	pager := pager.Create(path)
	// metaPage := pager.GetMetaPage()
	dm := &DataManager{
		pager: pager,
	}
	return dm
}

func Open(path string) *DataManager {
	pager := pager.Open(path)
	dm := &DataManager{
		pager: pager,
	}
	return dm
}

func getIndex(tableId uint16, columnId uint16) *bplustree.BPlusTree {
	return nil
}

func (dm *DataManager) SelectData(selectStatement *ast.SelectStatement) <-chan *ast.Record {
	return nil
}

func (dm *DataManager) InsertData(insertStatement *ast.InsertIntoStatement) {

}

// func (dm *DataManager) GetIndex(indexId uint16) *bplustree.BPlusTree {
// 	if dm.Indexs[indexId] == nil {
// 		dm.Indexs[indexId] = bplustree.NewTree(dm.pager, 16, 16)
// 	}
// 	return dm.Indexs[indexId]
// }
