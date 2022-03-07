/*
Double Write Bbuffer 用于保证单个页的完整性，以便 redo log 可以恢复。
Double Write Buffer 的结构如下：
inmemory:
	pages: map[pageNum]page
	bufferFile: file

ondisk:
	bufferFile: 用于缓存 inmemory 中的数据，每次写入时，
		先将 inmemory 中的数据写入 bufferFile，然后将 bufferFile 写入磁盘
	pageFile: 实际页文件
*/
package doublewrite

import (
	"bytes"
	"minidb-go/util"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type DoubleWrite struct {
	// 内存中的脏页，key 为页号，value 为页的字节数组，大小为 PAGE_SIZE
	pages map[util.UUID][]byte

	memoryLock sync.Mutex
	diskLock   sync.Mutex

	bufferFile *os.File
	// double write 不负责关闭 page file
	pageFile *os.File
}

func Open(path string, pageFile *os.File) *DoubleWrite {
	path = path + "/double_write.db"
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open double write file %s failed: %v", path, err)
	}

	return &DoubleWrite{
		pages:      make(map[util.UUID][]byte),
		bufferFile: file,
		pageFile:   pageFile,
	}
}

func Create(path string, pageFile *os.File) *DoubleWrite {
	path = path + "/double_write.db"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open double write file %s failed: %v", path, err)
	}
	initData := make([]byte, util.PAGE_SIZE*util.DOUBLE_WRITE_POOL_PAGE_NUM)
	file.Write(initData)
	return &DoubleWrite{
		pages:      make(map[util.UUID][]byte),
		bufferFile: file,
		pageFile:   pageFile,
	}
}

// 将页文件恢复到所有页均未损坏的状态，使之可以被 redo log 恢复
func Recover(path string, pageFile *os.File) {
	path = path + "/double_write.db"
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open double write file %s failed: %v", path, err)
	}

	// 先将 buffer 中的数据写入磁盘
	file.Seek(0, 0)
	for {
		page := make([]byte, util.PAGE_SIZE)
		n, err := file.Read(page)
		if err != nil {
			break
		}
		if n != util.PAGE_SIZE {
			log.Fatalf("read double write file %s failed: %v", path, err)
		}
		// 如果读入的数据全为 0，则该页后面的数据都是 0
		if bytes.Equal(page, make([]byte, util.PAGE_SIZE)) {
			break
		}
		// 如果 checkeSum 校验失败，说明在该页处写入 buffer 时发生了非正常退出，
		// 则当前页所对应的数据文件中的 page 一定是完好的，
		// 因为在写入时，是先将脏页写入磁盘中的 buffer，然后再将 buffer 写入磁盘中的 page
		if !IsValid(page) {
			break
		}
		pageNum := util.BytesToUUID(page[:4])
		pageFile.Seek(int64(pageNum*util.PAGE_SIZE), 0)
		pageFile.Write(page)
	}
}

// 将内存中的数据写入磁盘
func (dw *DoubleWrite) FlushToDisk() {
	// 分配一个 pages 的副本，并清空原 pages
	dw.memoryLock.Lock()
	pages := dw.pages
	dw.pages = make(map[util.UUID][]byte)
	dw.memoryLock.Unlock()

	// 先将内存中的脏页写入磁盘中的 buffer
	dw.diskLock.Lock()
	dw.bufferFile.Seek(0, 0)
	for _, page_bytes := range pages {
		dw.bufferFile.Write(page_bytes)
	}

	// 然后再将脏页写入磁盘中的 page
	for pageNum, page_bytes := range pages {
		dw.pageFile.Seek(int64(pageNum*util.PAGE_SIZE), 0)
		dw.pageFile.Write(page_bytes)
	}

	// 最后需要将 buffer 清空
	dw.bufferFile.Seek(0, 0)
	dw.bufferFile.Write(make([]byte, util.PAGE_SIZE*util.DOUBLE_WRITE_POOL_PAGE_NUM))

	dw.diskLock.Unlock()

	// TODO: 刷新 checkpoint
}

func (dw *DoubleWrite) Write(pageNum util.UUID, page []byte) error {
	dw.memoryLock.Lock()

	// 写入 checkSum 到 page 末尾
	pageCheckSum := CheckSum(page[:util.PAGE_SIZE-4])
	copy(page[util.PAGE_SIZE-4:], pageCheckSum)

	dw.pages[pageNum] = page

	dw.memoryLock.Unlock()
	return nil
}

func (dw *DoubleWrite) Close() {
	dw.FlushToDisk()
	dw.bufferFile.Close()
}

func CheckSum(page []byte) []byte {
	sum := make([]byte, 4)
	k := len(page) / 4
	for _, b := range page[:k] {
		sum[0] ^= b
	}
	for _, b := range page[k : k*2] {
		sum[1] ^= b
	}
	for _, b := range page[k*2 : k*3] {
		sum[2] ^= b
	}
	for _, b := range page[k*3:] {
		sum[3] ^= b
	}
	return sum
}

func IsValid(page []byte) bool {
	return bytes.Equal(CheckSum(page[:util.PAGE_SIZE-4]), page[util.PAGE_SIZE-4:])
}
