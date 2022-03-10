package serialization

import "minidb-go/tm"

type Transaction struct {
	XID      tm.XID
	snapshot map[tm.XID]struct{}
}

func newTransaction(xid tm.XID, activeTransaction map[tm.XID]*Transaction) *Transaction {
	transaction := &Transaction{
		XID:      xid,
		snapshot: make(map[tm.XID]struct{}),
	}
	for xid, transaction := range activeTransaction {
		transaction.snapshot[xid] = struct{}{}
	}
	return transaction
}
