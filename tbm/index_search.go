package tbm

import (
	"minidb-go/parser/ast"
	"minidb-go/parser/token"
	"minidb-go/storage/index"
	"minidb-go/util"
)

// 获取 where 对应的行的偏移量
func (tbm *TableManager) parseWhere(tableName string, where *ast.WhereStatement) (
	chan int64, error) {
	if where == nil {
		return nil, nil
	}
	if !where.IsExists {
		return tbm.getAllOffset(tableName)
	}
	return tbm.parseWhereExists(tableName, where)
}

// 全表扫描
func (tbm *TableManager) getAllOffset(tableName string) (chan int64, error) {
	return nil, nil
}

func (tbm *TableManager) parseWhereExists(tableName string,
	where *ast.WhereStatement) (chan int64, error) {
	expr := where.Expr
	if expr.Op == token.TT_EQUAL || expr.Op == token.TT_ASSIGN {
		return tbm.parseWhereEqual(tableName, where)
	} else {
		return tbm.parseWhereRange(tableName, where)
	}
}

// 相等查询
func (tbm *TableManager) parseWhereEqual(tableName string, where *ast.WhereStatement) (
	chan int64, error) {
	columnName := string(*where.Expr.Left.(*ast.SQLColumn))
	tableInfo := tbm.metaData.GetTableInfo(tableName)
	columnDefine := tableInfo.GetColumnDefine(columnName)
	index := columnDefine.Index
	if columnDefine.ColumnId == 0 {
		// 主键相等查找
		return tbm.parseWherePrimaryKey(index, where.Expr.Right)
	} else {
		// 非主键相等查找
		return tbm.parseWhereSimple(index, where.Expr.Right)
	}
}

// 主键相等查询
func (tbm *TableManager) parseWherePrimaryKey(primaryIndex index.Index, value ast.SQLExprValue) (
	chan int64, error) {
	val_chan := make(chan index.KeyType, 1)
	defer close(val_chan)
	val_chan <- value.Raw()
	offsets, _ := tbm.getOffsetsFromPrimaryKeys(primaryIndex, val_chan)
	return offsets, nil
}

// 非主键相等查询
func (tbm *TableManager) parseWhereSimple(simpleIndex index.Index, value ast.SQLExprValue) (
	chan int64, error) {
	if simpleIndex == nil {
		// 全盘扫描
	}
	primaryKeys, _ := tbm.getPrimaryKeys(simpleIndex, value)
	return tbm.getOffsetsFromPrimaryKeys(simpleIndex, primaryKeys)
}

func (tbm *TableManager) getOffsetsFromPrimaryKeys(
	primaryIndex index.Index, primaryKeys chan index.KeyType) (chan int64, error) {
	offsets := make(chan int64, 16)
	go func() {
		for key := range primaryKeys {
			if key == nil {
				continue
			}
			offsetValues := primaryIndex.Search(key)
			for offsetValue := range offsetValues {
				if offsetValue == nil {
					continue
				}
				offset := util.BytesToInt64(offsetValue)
				offsets <- offset
			}
		}
		close(offsets)
	}()
	return offsets, nil
}

// 根据非主键获取主键
func (tbm *TableManager) getPrimaryKeys(simpleIndex index.Index, value ast.SQLExprValue) (
	chan index.KeyType, error) {
	primaryKeys := make(chan index.KeyType, 16)
	// 查询主键
	go func() {
		for val := range simpleIndex.Search(value.Raw()) {
			if val == nil {
				continue
			}
			primaryKeys <- index.KeyType(val)
		}
	}()
	return primaryKeys, nil
}

// 范围查询
func (tbm *TableManager) parseWhereRange(tableName string,
	where *ast.WhereStatement) (chan int64, error) {
	return nil, nil
}
