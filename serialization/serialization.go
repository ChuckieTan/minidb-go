package serialization

import (
	"errors"
	"minidb-go/parser/ast"
	"minidb-go/serialization/tm"
	"minidb-go/storage"
	"sync"
)

var (
	ErrXidNotExists = errors.New("Transaction not exists")
)

// 使所有事务之间满足可重复读隔离，使用 MVCC 实现
type Serializer struct {
	transactionManager *tm.TransactionManager
	dataManager        *storage.DataManager

	activeTransaction map[tm.XID]*Transaction
	lock              sync.RWMutex
}

func Open(path string, dataManager *storage.DataManager) *Serializer {
	transactionManager := tm.Open(path)
	serializer := &Serializer{
		transactionManager: transactionManager,
		dataManager:        dataManager,
		activeTransaction:  make(map[tm.XID]*Transaction),
	}
	return serializer
}

func Create(path string, dataManager *storage.DataManager) *Serializer {
	transactionManager := tm.Create(path)
	serializer := &Serializer{
		transactionManager: transactionManager,
		dataManager:        dataManager,
		activeTransaction:  make(map[tm.XID]*Transaction),
	}
	return serializer
}

func (s *Serializer) Begin() tm.XID {
	s.lock.Lock()
	defer s.lock.Unlock()

	xid := s.transactionManager.Begin()
	s.activeTransaction[xid] = newTransaction(xid, s.activeTransaction)
	return xid
}

func (s *Serializer) Commit(xid tm.XID) error {
	s.lock.Lock()
	if _, ok := s.activeTransaction[xid]; !ok {
		return ErrXidNotExists
	}
	delete(s.activeTransaction, xid)
	s.lock.Unlock()

	s.transactionManager.Commit(xid)
	return nil
}

func (s *Serializer) Abort(xid tm.XID) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.activeTransaction[xid]; !ok {
		return ErrXidNotExists
	}
	// 从 activeTransaction 中删除
	delete(s.activeTransaction, xid)
	s.transactionManager.Abort(xid)
	return nil
}

func (s *Serializer) Read(xid tm.XID, selectStmt ast.SelectStmt) ([]*ast.Row, error) {
	s.lock.RLock()
	transaction, ok := s.activeTransaction[xid]
	s.lock.RUnlock()
	if !ok {
		return nil, ErrXidNotExists
	}

	rows_chan, err := s.dataManager.SelectData(selectStmt)
	if err != nil {
		return nil, err
	}
	rows := make([]*ast.Row, 0)
	for row := range rows_chan {
		visible, err := isVisible(row, transaction, s.transactionManager)
		if err != nil {
			return nil, err
		}
		if visible {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (s *Serializer) Insert(xid tm.XID, insertStmt ast.InsertIntoStmt) ([]ast.SQLExprValue, error) {
	s.lock.Lock()
	_, ok := s.activeTransaction[xid]
	s.lock.Unlock()
	if !ok {
		return nil, ErrXidNotExists
	}
	insertStmt.Row = wrapData(insertStmt.Row, xid)
	s.dataManager.InsertData(insertStmt)
	return insertStmt.Row, nil
}

func wrapData(row []ast.SQLExprValue, xid tm.XID) []ast.SQLExprValue {
	xmin := ast.SQLInt(xid)
	row = append(row, &xmin)
	xmax := ast.SQLInt(tm.NIL_XID)
	row = append(row, &xmax)
	return row
}

func (s *Serializer) Delete(xid tm.XID, deleteStmt ast.DeleteStatement) ([]*ast.Row, error) {
	s.lock.RLock()
	_, ok := s.activeTransaction[xid]
	s.lock.RUnlock()
	if !ok {
		return nil, ErrXidNotExists
	}
	selectStmt := ast.SelectStmt{
		TableName:    deleteStmt.TableName,
		Where:        deleteStmt.Where,
		ResultColumn: []string{"*"},
	}
	row_chan, err := s.dataManager.SelectData(selectStmt)
	if err != nil {
		return nil, err
	}
	rows := make([]*ast.Row, 0)
	for row := range row_chan {
		row.SetXmax(xid)
		s.dataManager.PageFile().WriteAt(row.Encode(), int64(row.Offset()))
		rows = append(rows, row)
	}
	return rows, nil
}

/*
(XMIN == Ti and                      // created by Ti itself and
     (XMAX == NULL or                    // not deleted now or
))
or                                   // or
(XMIN is commited and                // created by a commited treansaction and
 XMIN < XID and                      // the transaction begin before Ti and
 XMIN is not in SP(Ti) and           // the transaction commited before Ti begin and
 (XMAX == NULL or                    // not deleted now or
  (XMAX != Ti and                    // deleted by another transaction but
   (XMAX is not commited or          // the transaction is not commtied now or
    XMAX > Ti or                     // begain after Ti or
    XMAX is in SP(Ti)                // not commited when Ti begain
))))
*/
func isVisible(row *ast.Row, transaction *Transaction,
	transactionManager *tm.TransactionManager) (bool, error) {
	// xmin 表示数据的创建时间
	xmin, err := row.Xmin()
	if err != nil {
		return false, err
	}
	// xmax 表示数据的失效时间
	xmax, err := row.Xmax()
	if err != nil {
		return false, err
	}
	// 当前事务的执行时间
	xid := transaction.Xid()

	// 数据是否被删除
	isDeleted := xmax != tm.NIL_XID
	// 如果是当前事务创建，且没被删除，则可见
	if xmin == xid && !isDeleted {
		return true, nil
	}

	// 如果数据行的创建时间比当前事务晚，则不可见
	if xmin > xid {
		return false, nil
	}
	// 如果创建数据行的事务还未结束，则不可见
	if !transactionManager.IsCommitted(xmin) {
		return false, nil
	}
	// 如果创建数据行的事务在当前事务开始的时候还未结束（即在当前事务的 snapshot 中），则不可见
	if transaction.InSnapshot(xmin) {
		return false, nil
	}
	// 如果 Xmin 在当前事务开始前已经提交，并且开始时间小于当前事务，才有可能可见，否则不可见

	// 如果当前数据没被删除，则可见
	if !isDeleted {
		return true, nil
	}
	// 如果数据已被当前事务删除，则不可见
	if xmax == xid {
		return false, nil
	}
	// 如果已被其他事务删除，但删除当前行的事务还未提交，则可见，否则不可见
	if !transactionManager.IsCommitted(xmax) {
		return true, nil
	}
	// 如果已被其他事务删除，但删除当前行的事务晚于当前事务，则可见
	if xmax > xid {
		return true, nil
	}
	// 如果已被其他事务删除，但在删除行的事务在当前事务开始的时候还未结束，则可见
	if transaction.InSnapshot(xmax) {
		return true, nil
	}

	// 其他情况都不可见
	return false, nil
}

func (s *Serializer) Close() {
	s.transactionManager.Close()
}
