/*
recinfo 用于获取recovery的信息，包括 checkpoint 和 是否正常退出
文件名为 REC_INFO_FILE_NAME
文件格式为：
	checkpoint int64
	exit bool

*/
package recinfo

import (
	"fmt"
	"log"
	"os"
	"sync"
)

const (
	REC_INFO_FILE_NAME = "recovery_info"
)

type RecoveryInfo struct {
	infoFile *os.File

	lock sync.RWMutex
}

func Create(path string) *RecoveryInfo {
	path = path + "/" + REC_INFO_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	return &RecoveryInfo{
		infoFile: file,
	}
}

func Open(path string) *RecoveryInfo {
	path = path + "/" + REC_INFO_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	return &RecoveryInfo{
		infoFile: file,
	}
}

func (info *RecoveryInfo) GetCheckPoint() (int64, error) {
	info.lock.RLock()
	defer info.lock.RUnlock()

	info.infoFile.Seek(0, 0)
	var lsn int64
	_, err := fmt.Fscanf(info.infoFile, "%d", &lsn)
	return lsn, err
}

func (info *RecoveryInfo) SetCheckPoint(lsn int64) error {
	info.lock.Lock()
	defer info.lock.Unlock()

	info.infoFile.Seek(0, 0)
	_, err := fmt.Fprintf(info.infoFile, "%d", lsn)
	info.infoFile.Sync()
	return err
}

func (info *RecoveryInfo) IsExit() (bool, error) {
	info.lock.RLock()
	defer info.lock.RUnlock()

	info.infoFile.Seek(0, 0)
	var exit bool
	_, err := fmt.Fscanf(info.infoFile, "%t", &exit)
	return exit, err
}

func (info *RecoveryInfo) SetExit(exit bool) error {
	info.lock.Lock()
	defer info.lock.Unlock()

	info.infoFile.Seek(0, 0)
	_, err := fmt.Fprintf(info.infoFile, "%t", exit)
	info.infoFile.Sync()
	return err
}

// 关闭 recinfo，需要在最后关闭，并在关闭之前写入最新的 LSN
func (info *RecoveryInfo) Close() {
	info.SetExit(true)
	info.infoFile.Close()
}
