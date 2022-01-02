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
	"minidb-go/storage/index"
	"minidb-go/storage/pager"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/util"
	"sync"

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

func (dm *DataManager) SelectData(selectStatement *ast.SelectStatement) (
	<-chan ast.Row, error) {
	rows := make(chan ast.Row, 64)
	metaData := dm.pager.GetMetaData()
	// 获取表信息
	tableInfo := metaData.GetTableInfo(selectStatement.TableSource)
	if tableInfo == nil {
		close(rows)
		err := fmt.Errorf("table %s not exist", selectStatement.TableSource)
		return rows, err
	}

	if selectStatement.Where.IsExists {
		expr := selectStatement.Where.Expr
		if expr.IsEqual() {
			// 相等查找
			dm.equalSearch(rows, tableInfo, expr, expr.Right)
		} else {
			// TODO: 非相等查找
			err := fmt.Errorf("only support equal condition")
			return rows, err
		}
	} else {
		// 没有 where 条件，全表扫描
		dm.fullScan(rows, tableInfo, whereToFunc(selectStatement.Where.Expr))
	}
	return rows, nil
}

func whereToFunc(expr ast.SQLExpr) func(ast.Row) bool {
	panic("implement me")
	return func(row ast.Row) bool {
		return false
	}
}

func (dm *DataManager) equalSearch(rows chan<- ast.Row, tableInfo *pagedata.TableInfo,
	expr ast.SQLExpr, value ast.SQLExprValue) {
	if expr.Left.ValueType() == ast.SQL_COLUMN {
		// 查询索引
		columnName := string(expr.Left.(ast.SQLColumn))
		columnDefine := tableInfo.GetColumnDefine(columnName)
		if columnDefine == nil {
			close(rows)
			log.Errorf("column %s not exist", columnName)
			return
		}
		index := columnDefine.Index
		if index == nil {
			// 没有索引，全表扫描
			log.Warnf("index %s not exist, full scan table", expr.Left.(ast.SQLColumn))
			dm.fullScan(rows, tableInfo, whereToFunc(expr))
			return
		}
		if columnName == tableInfo.PrimaryKey() {
			// 主键索引，直接遍历数据页
			dm.primaryKeyEqualSearch(rows, index, expr.Right)
			return
		} else {
			// 非主键索引相等
			primaryColumn := tableInfo.GetColumnDefine(tableInfo.PrimaryKey())
			if primaryColumn == nil {
				close(rows)
				log.Errorf("primary key %s not exist", tableInfo.PrimaryKey())
				return
			}
			primaryIndex := primaryColumn.Index
			if primaryIndex == nil {
				close(rows)
				log.Errorf("fatal error: primary index %s not exist", tableInfo.PrimaryKey())
				return
			}
			dm.simpleEqualSearch(rows, index, primaryIndex, expr.Right)
			return
		}
	} else {
		if expr.Left == expr.Right {
			log.Warnf("full scan table")
			dm.fullScan(rows, tableInfo, whereToFunc(expr))
			return
		} else {
			// 左值和右值不相等，结果为空
			close(rows)
			return
		}
	}
}

func (dm *DataManager) primaryKeyEqualSearch(rows chan<- ast.Row, primaryIndex index.Index, value ast.SQLExprValue) {
	valueChan := primaryIndex.Search(value.Raw())
	w := sync.WaitGroup{}
	w.Add(util.MAX_SEARCH_THRESHOLD)
	for i := 0; i < util.MAX_SEARCH_THRESHOLD; i++ {
		go func() {
			defer w.Done()
			for pageNumBytes := range valueChan {
				pageNum := util.BytesToUUID(pageNumBytes)
				dm.traverseData(rows, pageNum, value)
			}
		}()
	}
	go func() {
		w.Wait()
		close(rows)
	}()
	return
}

// 非主键索引相等查找
func (dm *DataManager) simpleEqualSearch(rows chan<- ast.Row, simpleIndex index.Index,
	primaryIndex index.Index, value ast.SQLExprValue) {
	// 先查找主键索引
	indexChan := simpleIndex.Search(value.Raw())
	w := sync.WaitGroup{}
	w.Add(util.MAX_SEARCH_THRESHOLD)
	for i := 0; i < util.MAX_SEARCH_THRESHOLD; i++ {
		go func() {
			defer w.Done()
			// 根据主键查找数据页
			for primaryKeyBytes := range indexChan {
				pageNumChan := primaryIndex.Search(index.KeyType(primaryKeyBytes))
				for pageNumBytes := range pageNumChan {
					pageNum := util.BytesToUUID(pageNumBytes)
					dm.traverseData(rows, pageNum, value)
				}
			}
		}()
	}
	go func() {
		w.Wait()
		close(rows)
	}()
	return
}

func (dm *DataManager) fullScan(rows chan<- ast.Row, tableInfo *pagedata.TableInfo, check func(ast.Row) bool) {

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
	metaData := dm.pager.GetMetaData()
	tableInfo := metaData.GetTableInfo(insertStatement.TableName)
	if tableInfo == nil {
		log.Warnf("table %s not exist", insertStatement.TableName)
		return
	}
	// TODO: 检查字段是否存在
	dataPage, err := dm.pager.Select(insertStatement.Row.Len(), insertStatement.TableName)
	if err != nil {
		log.Errorf("table %s not exist", insertStatement.TableName)
		return
	}
	// 插入数据
	pageData := dataPage.Data().(*pagedata.RecordData)
	pageData.Append(insertStatement.Row)

	// for _, columnDefine := range tableInfo.ColumnDefines() {
	// 	index := columnDefine.Index
	// }
}
