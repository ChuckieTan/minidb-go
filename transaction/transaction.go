/* 事务管理器
事务管理器使用单例模式，即整个程序中只有一个事务管理器
每个事务在任意时刻只能处于三种状态中的一个，分别是：
TRANS_ACTIVE		正在生效
TRANS_COMMITED		已提交
TRANS_ABORTED		已撤销
*/
package transaction

import "os"

type XID uint32

// 事务的三种状态
const (
	TRANS_ACTIVE = iota
	TRANS_COMMITED
	TRANS_ABORTED
)

type TransactionManager interface {
	Open()

	Begin() XID
	Commit(XID)
	Abort(XID)

	IsActive(XID) bool
	IsCommit(XID) bool
	IsAbort(XID) bool
}

var manager *transactionManager

// 事务管理器
type transactionManager struct {
	// XID文件
	file       os.File
	xidCounter XID
}

func (tm *transactionManager) Open(path string) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return
	}
	tm.file = *file
}
