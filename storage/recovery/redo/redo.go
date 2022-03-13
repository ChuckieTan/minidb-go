package redo

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"

	"minidb-go/storage/recovery/redo/redolog"
)

type Redo struct {
	redoFile *os.File
	// 数据页文件，redo 不负责关闭
	pageFile *os.File
	LSN      int64

	lock sync.RWMutex
}

const (
	REDO_LOG_FILE_NAME = "redo.log"
)

func Create(path string, pageFile *os.File) *Redo {
	path = path + "/" + REDO_LOG_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	redo := &Redo{
		redoFile: file,
		pageFile: pageFile,
		LSN:      0,
	}
	return redo
}

func Open(path string, pageFile *os.File) *Redo {
	path = path + "/" + REDO_LOG_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	stat, _ := file.Stat()
	redo := &Redo{
		redoFile: file,
		pageFile: pageFile,
		LSN:      stat.Size(),
	}
	return redo
}

// 添加一系列 redo log，并返回最后一个 redo log 的 LSN
func (redo *Redo) Append(logs []redolog.Log) (int64, error) {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	buf := new(bytes.Buffer)
	buf.Grow(512)
	for _, log := range logs {
		err := redo.write(log, buf)
		if err != nil {
			return 0, err
		}
	}
	redo.redoFile.Write(buf.Bytes())
	redo.redoFile.Sync()
	return redo.LSN, nil
}

func (redo *Redo) write(log redolog.Log, w io.Writer) error {
	log.SetLSN(redo.LSN)
	raw := log.Bytes()
	redo.LSN += int64(len(raw))
	_, err := w.Write(raw)
	if err != nil {
		return err
	}
	return nil
}

func (redo *Redo) Recover(beginLSN int64) {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	redo.redoFile.Seek(beginLSN, 0)
	for {
		// log, err := redolog.Read(redo.redoFile)
		// if err != nil {
		// 	if err == io.EOF {
		// 		break
		// 	}
		// 	log.Printf("read redo log failed: %v", err)
		// 	break
		// }
		// redo.LSN = log.LSN()
		// redo.apply(log)
	}
}

func (redo *Redo) Close() {
	redo.redoFile.Close()
}
