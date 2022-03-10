package serialization

import (
	"errors"
	"minidb-go/storage"
	"minidb-go/storage/recovery"
	"minidb-go/tm"
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

func Open(path string, rec *recovery.Recovery) *Serializer {
	dataManager := storage.Open(path, rec)
	transactionManager := tm.Open(path)
	serializer := &Serializer{
		transactionManager: transactionManager,
		dataManager:        dataManager,
		activeTransaction:  make(map[tm.XID]*Transaction),
	}
	return serializer
}

func Create(path string, rec *recovery.Recovery) *Serializer {
	dataManager := storage.Create(path, rec)
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
