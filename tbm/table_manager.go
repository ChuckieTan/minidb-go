package tbm

import (
	"errors"
	"minidb-go/parser/ast"
	"minidb-go/serialization"
	"minidb-go/storage"
	"minidb-go/storage/bplustree"
	"minidb-go/storage/pager"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/storage/recovery"
	"minidb-go/tm"
	"minidb-go/util"
)

var ErrTableNotExists = errors.New("Table not exists")

type ResultList struct {
	columns []string
	rows    []*ast.Row
}

func (tbm *TableManager) NewResultList(tableName string, rows []*ast.Row) (*ResultList, error) {
	tableInfo := tbm.metaData.GetTableInfo(tableName)
	if tableInfo == nil {
		return nil, ErrTableNotExists
	}
	columns := tableInfo.ColumnNames()
	return &ResultList{
		columns: columns,
		rows:    rows,
	}, nil
}

type TableManager struct {
	metaData *pagedata.MetaData

	pager      *pager.Pager
	rec        *recovery.Recovery
	serializer *serialization.Serializer
}

func Create(path string) *TableManager {
	pager := pager.Create(path)
	rec := recovery.Create(path, pager.PageFile())
	dataManager := storage.Create(path, pager, rec)
	serializer := serialization.Create(path, dataManager)
	tbm := &TableManager{
		metaData:   pager.GetMetaData(),
		serializer: serializer,
		pager:      pager,
		rec:        rec,
	}
	return tbm
}

func Open(path string) *TableManager {
	pager := pager.Open(path)
	rec := recovery.Open(path, pager.PageFile())
	dataManager := storage.Open(path, pager, rec)
	serializer := serialization.Open(path, dataManager)
	tbm := &TableManager{
		metaData:   pager.GetMetaData(),
		serializer: serializer,
		pager:      pager,
		rec:        rec,
	}
	return tbm
}

func (tbm *TableManager) Begin() tm.XID {
	return tbm.serializer.Begin()
}

func (tbm *TableManager) Commit(xid tm.XID) error {
	return tbm.serializer.Commit(xid)
}

func (tbm *TableManager) Abort(xid tm.XID) error {
	return tbm.serializer.Abort(xid)
}

func (tbm *TableManager) Select(xid tm.XID, selectStmt ast.SelectStmt) (*ResultList, error) {
	rows, err := tbm.serializer.Read(xid, selectStmt)
	if err != nil {
		return nil, err
	}
	return tbm.NewResultList(selectStmt.TableName, rows)
}

func (tbm *TableManager) Insert(xid tm.XID, insertStmt ast.InsertIntoStmt) (*ResultList, error) {
	rows := []*ast.Row{ast.NewRow(insertStmt.Row)}
	resultList, err := tbm.NewResultList(insertStmt.TableName, rows)
	if err != nil {
		return nil, err
	}
	return resultList, tbm.serializer.Insert(xid, insertStmt)
}

func (tbm *TableManager) Delete(xid tm.XID, deleteStmt ast.DeleteStatement) (*ResultList, error) {
	rows, err := tbm.serializer.Delete(xid, deleteStmt)
	if err != nil {
		return nil, err
	}
	return tbm.NewResultList(deleteStmt.TableName, rows)
}

func (tbm *TableManager) Update(xid tm.XID, updateStmt ast.UpdateStmt) (*ResultList, error) {
	// 先删除对应的行
	deleteStmt := ast.DeleteStatement{
		TableName: updateStmt.TableName,
		Where:     updateStmt.Where,
	}
	old_rows, err := tbm.serializer.Delete(xid, deleteStmt)
	if err != nil {
		return nil, err
	}
	if len(old_rows) == 0 {
		return nil, nil
	}

	// 再插入修改后的行
	columnIds := make([]uint16, len(updateStmt.ColumnAssignList))
	tableInfo := tbm.metaData.GetTableInfo(updateStmt.TableName)
	for i, columnAssign := range updateStmt.ColumnAssignList {
		columnIds[i] = tableInfo.GetColumnDefine(columnAssign.ColumnName).ColumnId
	}

	rows := make([]*ast.Row, 0)
	for _, row := range old_rows {
		insertValues := make([]ast.SQLExprValue, 0)
		util.DeepCopy(&insertValues, row.Data())
		for i, columnAssign := range updateStmt.ColumnAssignList {
			insertValues[columnIds[i]] = columnAssign.Value
		}
		insertStmt := ast.InsertIntoStmt{
			TableName: updateStmt.TableName,
			Row:       insertValues,
		}
		err := tbm.serializer.Insert(xid, insertStmt)
		rows = append(rows, ast.NewRow(insertValues))
		if err != nil {
			return nil, err
		}
	}
	return tbm.NewResultList(updateStmt.TableName, rows)
}

func (tbm *TableManager) CreateTable(xid tm.XID, createTableStmt ast.CreateTableStmt) error {
	tableInfo := new(pagedata.TableInfo)
	tableInfo.TableName = createTableStmt.TableName
	tableInfo.TableId = uint16(len(tbm.metaData.Tables))
	tableInfo.ColumnDefines = createTableStmt.ColumnDefines

	// 设置主键索引
	tableInfo.ColumnDefines[0].Index = bplustree.NewTree(
		tbm.pager, 8, 4, tableInfo.TableId, 0, tbm.rec,
	)

	// 初始化一个空数据页
	page := tbm.pager.NewPage(pagedata.NewRecordData())
	tableInfo.FirstPageNum = page.PageNum()
	tableInfo.LastPageNum = page.PageNum()

	tbm.metaData.AddTable(tableInfo)
	return nil
}
