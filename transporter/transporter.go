package transporter

import (
	"minidb-go/serialization/tm"
	"minidb-go/tbm"
)

type Request struct {
	Xid  tm.XID
	Stmt string
}

type Response struct {
	Xid        tm.XID
	ResultList *tbm.ResultList
	Err        string
}
