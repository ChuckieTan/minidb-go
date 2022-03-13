/*
Data Manager 的恢复机制实现，由 redo log 和 double write 实现
*/
package recovery

import (
	"minidb-go/storage/pager"
	"minidb-go/storage/recovery/doublewrite"
	"minidb-go/storage/recovery/recinfo"
	"minidb-go/storage/recovery/redo"
	"os"

	log "github.com/sirupsen/logrus"
)

type Recovery struct {
	redo     *redo.Redo
	dwrite   *doublewrite.DoubleWrite
	pageFile *os.File
	recinfo  *recinfo.RecoveryInfo
}

func Create(path string, pageFile *os.File) *Recovery {
	r := &Recovery{
		redo:     redo.Create(path, pageFile),
		dwrite:   doublewrite.Create(path, pageFile),
		pageFile: pageFile,
		recinfo:  recinfo.Create(path),
	}
	r.dwrite.SetCheckPoint = r.recinfo.SetCheckPoint
	return r
}

func Open(path string, pageFile *os.File) *Recovery {
	r := &Recovery{
		redo:     redo.Open(path, pageFile),
		dwrite:   doublewrite.Open(path, pageFile),
		pageFile: pageFile,
		recinfo:  recinfo.Open(path),
	}
	r.dwrite.SetCheckPoint = r.recinfo.SetCheckPoint

	// 判断是否需要恢复
	exit, err := r.recinfo.IsExit()
	if err != nil {
		log.Fatalf("check recovery info failed: %v", err)
	}
	if !exit {
		log.Warnf("database exited abnormally, need recovery")
		r.recover()
	}
	return r
}

func (r *Recovery) recover() {
	log.Info("recover begin")
	// 先恢复 Double Write，恢复部分写的页
	r.dwrite.Recover()
	// 然后再恢复 Redo
	LSN, err := r.recinfo.GetCheckPoint()
	if err != nil {
		log.Fatalf("get check point failed: %v", err)
	}
	r.redo.Recover(LSN)
	log.Info("recover end")
}

func (rec *Recovery) Write(page *pager.Page) {
	// LSN, _ := rec.redo.Append(page.Logs())
	// // 需要先更新页的 LSN，再写入 double write
	// page.LSN = LSN
	rec.dwrite.Write(page)
}

func (rec *Recovery) Close() {
	rec.redo.Close()
	rec.dwrite.Close()
	rec.recinfo.Close()
}
