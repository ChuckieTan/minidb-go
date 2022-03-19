package tablelock

import (
	"minidb-go/serialization/tm"
	"sync"
)

type TableLock struct {
	// 事务持有的数据行集合
	XidToRow map[tm.XID]map[int64]bool
	// 持有该数据行的事务
	RowToXid map[int64]tm.XID
	// 等待对应数据行的事务集合
	RowWaitXid map[int64]map[tm.XID]bool
	// 事务等待的数据行
	XidWaitRow map[tm.XID]int64
	// 对应事务的等待 channel
	XidWaitChans map[tm.XID]chan bool

	lock sync.Mutex
}

func New() *TableLock {
	return &TableLock{
		XidToRow:     make(map[tm.XID]map[int64]bool),
		RowToXid:     make(map[int64]tm.XID),
		RowWaitXid:   make(map[int64]map[tm.XID]bool),
		XidWaitRow:   make(map[tm.XID]int64),
		XidWaitChans: make(map[tm.XID]chan bool),
	}
}

func (tl *TableLock) initMap(xid tm.XID, row int64) {
	// 如果对应的 xid 从没添加过，则初始化
	if _, ok := tl.XidToRow[xid]; !ok {
		tl.XidToRow[xid] = make(map[int64]bool)
	}
	// 如果对应的 row 从没添加过，则初始化
	if _, ok := tl.RowWaitXid[row]; !ok {
		tl.RowWaitXid[row] = make(map[tm.XID]bool)
	}
}

// 添加一条 xid 到 row 的边
func (tl *TableLock) Add(xid tm.XID, row int64) (bool, chan bool) {
	tl.lock.Lock()
	defer tl.lock.Unlock()

	// 初始化
	tl.initMap(xid, row)

	// 如果对应的 xid 已经依赖，则返回 true
	if _, ok := tl.XidToRow[xid][row]; ok {
		return true, nil
	}

	// 如果 row 还没被占用，则返回 true
	if _, ok := tl.RowToXid[row]; !ok {
		// 让 xid 和 row 关联
		tl.XidToRow[xid][row] = true
		tl.RowToXid[row] = xid
		return true, nil
	}

	// 尝试将 xid->row 的边加入到等待图中，并判断是否有死锁
	tl.XidWaitRow[xid] = row
	tl.RowWaitXid[row][xid] = true
	if tl.hasDeadLock(xid) {
		// 如果有死锁，则删除 xid 和 row 的边
		delete(tl.XidToRow[xid], row)
		delete(tl.RowToXid, row)
		return false, nil
	}

	// 如果加入不会造成死锁，则返回 true，并设置等待的 channel，一直到对应的 row 释放
	ch := make(chan bool)
	tl.XidWaitChans[xid] = ch
	return true, ch
}

var visit map[tm.XID]bool

func (tl *TableLock) hasDeadLock(xid tm.XID) bool {
	visit = make(map[tm.XID]bool)
	return tl.dfs(xid)
}

func (tl *TableLock) dfs(xid tm.XID) bool {
	// 如果 xid 已经被访问过，则有循环依赖，返回 true
	if _, ok := visit[xid]; ok {
		return true
	}
	visit[xid] = true

	// 一个边到另一个边就是先找出当前 xid 在等待哪个 row，在找出那个 row 被哪个 xid 所拥有
	row, ok := tl.XidWaitRow[xid]
	// 如果当前 xid 没有在等待哪个 row，则无环
	if !ok {
		return false
	}
	nextXid, ok := tl.RowToXid[row]
	// 如果当前 row 没有被哪个 xid 所拥有，则无环
	if !ok {
		return false
	}
	return tl.dfs(nextXid)
}

// 移除 xid 对应的所有依赖
func (tl *TableLock) Remove(xid tm.XID) {
	tl.lock.Lock()
	defer tl.lock.Unlock()

	rows := tl.XidToRow[xid]
	for row := range rows {
		// 对事务依赖的每一个数据行 row，选择一个等待 row 的事务释放
		tl.selectNewXid(row)
	}
	// 删除事务对数据行的依赖
	delete(tl.XidToRow, xid)
	// 删除事务对数据行的等待
	delete(tl.XidWaitRow, xid)
	// 删除事务的等待 channel
	delete(tl.XidWaitChans, xid)
}

// 在函数里面数据行依赖的旧事务
// 选择对应 row 的一个事务，并将其从等待图中移除
func (tl *TableLock) selectNewXid(row int64) {
	// 删除数据行依赖的旧事务
	delete(tl.RowToXid, row)
	for xid := range tl.RowWaitXid[row] {
		if _, ok := tl.XidWaitRow[xid]; !ok {
			// 删除 xid 对 row 的等待
			delete(tl.RowWaitXid[row], xid)
			// 如果这个事务已经被撤销，则选择下一个
			continue
		} else {
			tl.RowToXid[row] = xid
			tl.XidToRow[xid][row] = true
			// 删除 xid 对应的等待 row
			delete(tl.XidWaitRow, xid)
			// 删除 xid 对 row 的等待
			delete(tl.RowWaitXid[row], xid)

			ch := tl.XidWaitChans[xid]
			// 删除 xid 对应的等待 channel
			delete(tl.XidWaitChans, xid)
			ch <- true
			break
		}
	}
	// 删除占用的空间
	if len(tl.RowWaitXid[row]) == 0 {
		delete(tl.RowWaitXid, row)
	}
}
