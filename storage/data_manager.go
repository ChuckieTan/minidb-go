/*
DataManager 负责管理数据文件，提供数据和索引的读写操作
DataManager 中的数据没有版本号的概念，每次选择出的数据都是所有版本的数据
DataManager 中的数据具有持久性，在异常退出后会自动进行恢复
	该特性是由 Redo Log 和 double write 共同支持的
*/
package storage

import (
	"errors"
	"fmt"
	"minidb-go/parser/ast"
	"minidb-go/storage/bplustree"
	"minidb-go/storage/pagedata"
	"minidb-go/storage/pager"

	log "github.com/sirupsen/logrus"
)

var (
	ErrTableNotExist = errors.New("table not exist")
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

func (dm *DataManager) SelectData(selectStatement *ast.SelectStatement) (
	recordChan <-chan *ast.Record, err error) {
	metaPage, err := dm.pager.GetMetaPage()
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	metaData := metaPage.Data().(*pagedata.MetaData)
	// 获取表信息
	tableInfo := metaData.GetTableInfo(selectStatement.TableSource)
	if tableInfo == nil {
		err = fmt.Errorf("table %s not exist", selectStatement.TableSource)
		return
	}

	if selectStatement.Where.IsExists {
		indexs := tableInfo.Indexs()
		expr := selectStatement.Where.Expr
		if expr.IsEqual() {
			columnName := string(expr.Left.(ast.SQLColumn))
			index, ok := indexs[columnName]
			if !ok {
				// TODO: 没有匹配的索引应该全表扫描
				err = fmt.Errorf("index %s not exist", columnName)
				return
			}
			// 查询索引
			recordChan = index.Select(expr.Right.Value)
		} else {
			err = fmt.Errorf("only support equal condition")
			return
		}
	}

	return
}

func (dm *DataManager) InsertData(insertStatement *ast.InsertIntoStatement) {

}
