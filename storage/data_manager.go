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
	"minidb-go/storage/index"
	"minidb-go/storage/pager"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/util"

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

func (dm *DataManager) getRecordData(pageNum util.UUID) *pagedata.RecordData {
	recordPage, err := dm.pager.GetPage(pageNum, pagedata.NewRecordData())
	if err != nil {
		log.Fatalf("get record page failed: %v", err)
	}
	return recordPage.Data().(*pagedata.RecordData)
}

func getIndex(tableId uint16, columnId uint16) *bplustree.BPlusTree {
	return nil
}

func (dm *DataManager) SelectData(selectStatement *ast.SelectStatement) (
	<-chan ast.Row, error) {
	rows := make(chan ast.Row)
	metaPage := dm.pager.GetMetaPage()
	metaData := metaPage.Data().(*pagedata.MetaData)
	// 获取表信息
	tableInfo := metaData.GetTableInfo(selectStatement.TableSource)
	if tableInfo == nil {
		close(rows)
		err := fmt.Errorf("table %s not exist", selectStatement.TableSource)
		return rows, err
	}

	if selectStatement.Where.IsExists {
		indexs := tableInfo.Indexs()
		expr := selectStatement.Where.Expr
		if expr.IsEqual() {
			// 相等查找
			if expr.Left.ValueType() == ast.SQL_COLUMN {
				columnName := string(expr.Left.(ast.SQLColumn))
				indexInfo, ok := indexs[columnName]
				if !ok {
					// TODO: 没有匹配的索引应该全表扫描
					err := fmt.Errorf("index %s not exist", columnName)
					return rows, err
				}
				// 查询索引
				valueChan := indexInfo.Index.Search(expr.Right.Raw())
				if indexInfo.IndexType == index.PRIMARY_INDEX {
					// 主键索引，直接遍历数据页
					for i := 0; i < 2; i++ {
						go func() {
							for pageNumBytes := range valueChan {
								pageNum := util.BytesToUUID(pageNumBytes)
								dm.traverseData(rows, pageNum, expr.Right)
							}
						}()
					}
					return rows, nil
				} else {
					// 非主键索引，需要先查找主键索引，再遍历数据页
					indexChan := indexInfo.Index.Search(expr.Right.Raw())

				}
			} else {
				if expr.Left == expr.Right {
					// TODO: 全表扫描
				} else {
					close(rows)
					return rows, nil
				}
			}
		} else {
			err := fmt.Errorf("only support equal condition")
			return rows, err
		}
	}
	return rows, nil
}

func (dm *DataManager) searchByIndex(rows <-chan ast.Row, index *index.Index, value string) {
	return
}

// 遍历数据页，查找符合条件的数据
func (dm *DataManager) traverseData(rows chan<- ast.Row, pageNum util.UUID, primaryKey ast.SQLExprValue) {
	recordData := dm.getRecordData(pageNum)
	for _, row := range recordData.Rows() {
		if row[0] == primaryKey {
			rows <- row
		}
	}
}

func (dm *DataManager) InsertData(insertStatement *ast.InsertIntoStatement) {

}
