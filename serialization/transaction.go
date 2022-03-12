package serialization

import "minidb-go/serialization/tm"

type Transaction struct {
	xid      tm.XID
	snapshot map[tm.XID]struct{}
}

func newTransaction(xid tm.XID, activeTransaction map[tm.XID]*Transaction) *Transaction {
	transaction := &Transaction{
		xid:      xid,
		snapshot: make(map[tm.XID]struct{}),
	}
	for xid, transaction := range activeTransaction {
		transaction.snapshot[xid] = struct{}{}
	}
	return transaction
}

func (transaction *Transaction) Xid() tm.XID {
	return transaction.xid
}

func (transaction *Transaction) Snapshot() map[tm.XID]struct{} {
	return transaction.snapshot
}

func (transaction *Transaction) InSnapshot(xid tm.XID) bool {
	_, ok := transaction.snapshot[xid]
	return ok
}
