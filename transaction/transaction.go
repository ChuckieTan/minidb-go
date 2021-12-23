/* 事务管理器
每个事务在任意时刻只能处于三种状态中的一个，分别是：
TRANS_ACTIVE		正在生效
TRANS_COMMITED		已提交
TRANS_ABORTED		已撤销
事务的状态用 1 个字节存储
*/
package transaction

import (
	"encoding/binary"
	"os"

	log "github.com/sirupsen/logrus"
)

type XID uint32

// 事务的三种状态
const (
	TRANS_ACTIVE byte = iota
	TRANS_COMMITED
	TRANS_ABORTED
)

const (
	MINIDB_XID_HEADER    = "Minidb XID file"
	XID_FILE_HEADER_SIZE = 4
	XID_FILE_SUFFIX      = ".xid"
)

// type TransactionManager interface {
// 	Create()
// 	Open()
// 	Close()

// 	Begin() XID
// 	Commit(XID)
// 	Abort(XID)

// 	IsActive(XID) bool
// 	IsCommit(XID) bool
// 	IsAbort(XID) bool
// }

// 事务管理器
type TransactionManager struct {
	// XID文件
	file       *os.File
	xidCounter XID
}

func Create(path string) (tm *TransactionManager) {
	file, err := os.OpenFile(path+XID_FILE_SUFFIX, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
		return
	}
	tm.file = file
	tm.xidCounter = 1
	return
}

func Open(path string) (tm *TransactionManager) {
	file, err := os.OpenFile(path+XID_FILE_SUFFIX, os.O_RDWR, 0600)
	if err != nil {
		log.Fatal(err)
		return
	}
	tm.file = file
	counterBytes := make([]byte, 4)
	n, err := file.ReadAt(counterBytes, 0)
	if n != 4 {
		log.Fatal("invalid xid file")
	}
	if err != nil {
		log.Fatal(err)
	}
	tm.xidCounter = XID(binary.BigEndian.Uint32(counterBytes))
	return
}

func (tm *TransactionManager) Close() {
	tm.file.Close()
}

func xidPosition(xid XID) (position uint32) {
	position = uint32(xid) - 1 + XID_FILE_HEADER_SIZE
	return
}

func (tm *TransactionManager) updateXID(xid XID, status byte) {
	offset := xidPosition(xid)
	statusBytes := make([]byte, 1)
	statusBytes[0] = status
	n, err := tm.file.WriteAt(statusBytes, int64(offset))
	if n != 1 {
		log.Fatal("unknown xid file write error")
	}
	if err != nil {
		log.Fatal(err)
	}
	err = tm.file.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func xidToBytes(xid XID) (slice []byte) {
	slice = make([]byte, 4)
	binary.BigEndian.PutUint32(slice, uint32(xid))
	return
}

func (tm *TransactionManager) incXidCounter() {
	tm.xidCounter++
	_, err := tm.file.WriteAt(xidToBytes(tm.xidCounter), 0)
	if err != nil {
		log.Fatal(err)
	}
	err = tm.file.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

// 开始一个事务，并返回该事务对应的 XID
func (tm *TransactionManager) Begin() (xid XID) {
	xid = tm.xidCounter + 1
	tm.updateXID(xid, TRANS_ACTIVE)
	tm.incXidCounter()
	return
}

// 提交一个事务
func (tm *TransactionManager) Commit(xid XID) {
	tm.updateXID(xid, TRANS_COMMITED)
}

// 回滚一个事务
func (tm *TransactionManager) Abort(xid XID) {
	tm.updateXID(xid, TRANS_ABORTED)
}

func (tm *TransactionManager) checkStatus(xid XID, status byte) (res bool) {
	statusBytes := make([]byte, 1)
	_, err := tm.file.ReadAt(statusBytes, int64(xidPosition(xid)))
	if err != nil {
		log.Fatal(err)
	}
	res = statusBytes[0] == status
	return
}

func (tm *TransactionManager) IsActive(xid XID) (res bool) {
	res = tm.checkStatus(xid, TRANS_ACTIVE)
	return
}

func (tm *TransactionManager) IsCommit(xid XID) (res bool) {
	res = tm.checkStatus(xid, TRANS_COMMITED)
	return
}

func (tm *TransactionManager) IsAbort(xid XID) (res bool) {
	res = tm.checkStatus(xid, TRANS_ABORTED)
	return
}
