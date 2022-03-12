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
	"minidb-go/parser/token"
	"minidb-go/storage/index"
	"minidb-go/storage/pager"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/storage/recovery"
	"minidb-go/storage/recovery/redo/redolog"
	"minidb-go/util"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	ErrTableNotExist = errors.New("table not exist")
)

type DataManager struct {
	pager *pager.Pager

	// recovery 在创建时传入，不负责关闭
	recovery *recovery.Recovery
	//TODO: Data Cache，自适应哈希索引
}

func Create(path string, p *pager.Pager, recovery *recovery.Recovery) *DataManager {
	dm := &DataManager{
		pager:    p,
		recovery: recovery,
	}
	dm.pager.SetCacheEviction(func(key interface{}, val interface{}) {
		page := val.(*pager.Page)
		dm.recovery.Write(page)
	})
	return dm
}

func Open(path string, p *pager.Pager, recovery *recovery.Recovery) *DataManager {
	dm := &DataManager{
		pager:    p,
		recovery: recovery,
	}
	dm.pager.SetCacheEviction(func(key interface{}, val interface{}) {
		page := val.(*pager.Page)
		dm.recovery.Write(page)
	})
	return dm
}

func (dm *DataManager) PageFile() *os.File {
	return dm.pager.PageFile()
}

func (dm *DataManager) getRecordData(pageNum util.UUID) *pagedata.RecordData {
	recordPage, err := dm.pager.GetPage(pageNum, pagedata.NewRecordData())
	if err != nil {
		log.Fatalf("get record page failed: %v", err)
	}
	return recordPage.Data().(*pagedata.RecordData)
}

func (dm *DataManager) SelectData(selectStatement ast.SelectStmt) (
	<-chan *ast.Row, error) {
	rows := make(chan *ast.Row, 64)
	metaData := dm.pager.GetMetaData()
	// 获取表信息
	tableInfo := metaData.GetTableInfo(selectStatement.TableName)
	if tableInfo == nil {
		close(rows)
		err := fmt.Errorf("table %s not exist", selectStatement.TableName)
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
		dm.fullScan(rows, tableInfo, selectStatement.Where.Expr)
	}
	return rows, nil
}

// 把 where 转换成一个函数，返回值为 bool，表示是否符合条件
func whereToFunc(tableInfo *pagedata.TableInfo, expr *ast.SQLExpr) (
	func(row *ast.Row) bool, error,
) {
	if expr == nil {
		return func(row *ast.Row) bool {
			return true
		}, nil
	}
	if expr.Left.ValueType() == ast.SQL_COLUMN &&
		expr.Right.ValueType() == ast.SQL_COLUMN {
		// 两个都是列名
		columnNameL := string(*expr.Left.(*ast.SQLColumn))
		columnNameR := string(*expr.Right.(*ast.SQLColumn))
		columnDefines := tableInfo.ColumnDefines
		indexL, indexR := -1, -1
		for i, columnDefine := range columnDefines {
			if columnDefine.Name == columnNameL {
				indexL = i
			}
			if columnDefine.Name == columnNameR {
				indexR = i
			}
		}
		if indexL == -1 {
			return nil, fmt.Errorf("column %s not exist", columnNameL)
		}
		if indexR == -1 {
			return nil, fmt.Errorf("column %s not exist", columnNameR)
		}
		switch expr.Op {
		case token.TT_EQUAL:
			return func(row *ast.Row) bool {
				return row.Data[indexL] == row.Data[indexR]
			}, nil
		case token.TT_NOT_EQUAL:
			return func(row *ast.Row) bool {
				return row.Data[indexL] != row.Data[indexR]
			}, nil
		// TODO: 大于小于比较
		default:
			return nil, fmt.Errorf("operator %v not support", expr.Op)
		}
	} else if expr.Left.ValueType() == ast.SQL_COLUMN {
		columnIndex := -1
		for i, columnDefine := range tableInfo.ColumnDefines {
			if columnDefine.Name == string(*expr.Left.(*ast.SQLColumn)) {
				columnIndex = i
				break
			}
		}
		if columnIndex == -1 {
			return nil, fmt.Errorf("column %s not exist", *expr.Left.(*ast.SQLColumn))
		}
		switch expr.Op {
		case token.TT_EQUAL:
			return func(row *ast.Row) bool {
				return row.Data[columnIndex] == expr.Right
			}, nil
		case token.TT_NOT_EQUAL:
			return func(row *ast.Row) bool {
				return row.Data[columnIndex] != expr.Right
			}, nil
		default:
			return nil, fmt.Errorf("operator %v not support", expr.Op)
		}
	} else {
		// 两边都是常量，直接比较
		switch expr.Op {
		case token.TT_EQUAL:
			return func(row *ast.Row) bool {
				return expr.Left == expr.Right
			}, nil
		case token.TT_NOT_EQUAL:
			return func(row *ast.Row) bool {
				return expr.Left != expr.Right
			}, nil
		default:
			return nil, fmt.Errorf("operator %v not support", expr.Op)
		}
	}
}

func (dm *DataManager) equalSearch(rows chan<- *ast.Row, tableInfo *pagedata.TableInfo,
	expr *ast.SQLExpr, value ast.SQLExprValue) {
	if expr.Left.ValueType() == ast.SQL_COLUMN {
		// 查询索引
		columnName := string(*expr.Left.(*ast.SQLColumn))
		columnDefine := tableInfo.GetColumnDefine(columnName)
		if columnDefine == nil {
			close(rows)
			log.Errorf("column %s not exist", columnName)
			return
		}
		index := columnDefine.Index
		if index == nil {
			// 没有索引，全表扫描
			log.Warnf("index %s not exist, full scan table", *expr.Left.(*ast.SQLColumn))
			dm.fullScan(rows, tableInfo, expr)
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
			dm.fullScan(rows, tableInfo, expr)
			return
		} else {
			// 左值和右值不相等，结果为空
			close(rows)
			return
		}
	}
}

func checkValueFunc(value ast.SQLExprValue) func(*ast.Row) bool {
	return func(row *ast.Row) bool {
		if len(row.Data) == 0 {
			return false
		}
		return ast.SQLValueEqual(row.Data[0], value)
	}
}

func (dm *DataManager) primaryKeyEqualSearch(rows chan<- *ast.Row, primaryIndex index.Index, value ast.SQLExprValue) {
	valueChan := primaryIndex.Search(value.Raw())
	w := sync.WaitGroup{}
	w.Add(util.MAX_SEARCH_THRESHOLD)
	for i := 0; i < util.MAX_SEARCH_THRESHOLD; i++ {
		go func() {
			defer w.Done()
			for pageNumBytes := range valueChan {
				pageNum := util.BytesToUUID(pageNumBytes)
				dm.traverseData(rows, pageNum, checkValueFunc(value))
			}
		}()
	}
	go func() {
		w.Wait()
		close(rows)
	}()
}

// 非主键索引相等查找
func (dm *DataManager) simpleEqualSearch(rows chan<- *ast.Row, simpleIndex index.Index,
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
					dm.traverseData(rows, pageNum, checkValueFunc(value))
				}
			}
		}()
	}
	go func() {
		w.Wait()
		close(rows)
	}()
}

// 扫描指定表的全部数据，自动关闭 rows
func (dm *DataManager) fullScan(rows chan<- *ast.Row, tableInfo *pagedata.TableInfo, expr *ast.SQLExpr) {
	// TODO: 双线程扫描
	check, err := whereToFunc(tableInfo, expr)
	if err != nil {
		close(rows)
		log.Errorf("whereToFunc failed: %v", err)
		return
	}
	pageNum := tableInfo.FirstPageNum
	for pageNum != 0 {
		dm.traverseData(rows, pageNum, check)
		var err error
		pageNum, err = dm.pager.NextPageNum(pageNum)
		if err != nil {
			log.Errorf("fatal error: %s", err)
			return
		}
	}
	close(rows)
}

// 遍历数据页，查找符合条件的数据，不负责关闭 rows
func (dm *DataManager) traverseData(rows chan<- *ast.Row, pageNum util.UUID, check func(*ast.Row) bool) {
	recordData := dm.getRecordData(pageNum)
	for _, row := range recordData.Rows() {
		if check(row) {
			rows <- row
		}
	}
}

// 插入数据
func (dm *DataManager) InsertData(insertStatement ast.InsertIntoStmt) {
	metaData := dm.pager.GetMetaData()
	tableInfo := metaData.GetTableInfo(insertStatement.TableName)
	if tableInfo == nil {
		log.Warnf("table %s not exist", insertStatement.TableName)
		return
	}
	row := ast.NewRow(insertStatement.Row)
	// TODO: 检查字段是否存在
	dataPage, err := dm.pager.Select(row.Size, insertStatement.TableName)
	row.SetOffset(uint64(dataPage.Size()))
	if err != nil {
		log.Errorf("table %s not exist", insertStatement.TableName)
		return
	}
	// 插入数据
	pageData := dataPage.Data().(*pagedata.RecordData)
	pageData.Append(row)

	redolog := redolog.NewRecordPageAppendLog(dataPage.PageNum(), row)
	dataPage.AppendLog(redolog)
	dm.recovery.Write(dataPage)

	for i, columnDefine := range tableInfo.ColumnDefines {
		index := columnDefine.Index
		if index == nil {
			continue
		}
		// 更新索引
		if i == 0 {
			// 主键索引
			index.Insert(insertStatement.Row[i].Raw(), util.UUIDToBytes(index.ValueSize(), dataPage.PageNum()))
		} else {
			// 非主键索引
			index.Insert(insertStatement.Row[i].Raw(), insertStatement.Row[0].Raw())
		}
	}
}

func (dm *DataManager) Close() {

}
