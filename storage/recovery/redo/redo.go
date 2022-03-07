package redo

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"
)

type Redo struct {
	redoFile *os.File
	// 数据页文件，redo 不负责关闭
	pageFile *os.File
	LSN      int64

	lock sync.RWMutex
}

func Create(path string, pageFile *os.File) *Redo {
	path = path + "/redo.log"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
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
	path = path + "/redo.log"
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
func (redo *Redo) Append(logs []*Log) (int64, error) {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	buf := new(bytes.Buffer)
	for _, log := range logs {
		err := redo.write(log, buf)
		if err != nil {
			return 0, err
		}
	}
	redo.redoFile.Write(buf.Bytes())
	redo.redoFile.Sync()
	redo.LSN += int64(buf.Len())
	return redo.LSN, nil
}

func (redo *Redo) write(log *Log, w io.Writer) error {
	_, err := w.Write(log.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (redo *Redo) Recover(beginLSN int64) error {
	redo.lock.Lock()
	defer redo.lock.Unlock()
	redo.redoFile.Seek(0, 0)
	for {
		buf := make([]byte, 1024)
		n, err := redo.redoFile.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalf("read redo file failed: %v", err)
			}
		}
		redo.pageFile.Write(buf[:n])
		redo.pageFile.Sync()
	}
	return nil
}

func (redo *Redo) Close() {
	redo.redoFile.Close()
}
