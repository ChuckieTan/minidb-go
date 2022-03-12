package pager

import (
	"fmt"
	"io"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/util"
	"minidb-go/util/cache"
	"minidb-go/util/cache/lru"
	"os"

	log "github.com/sirupsen/logrus"
)

type Pager struct {
	cache cache.Cache
	file  *os.File
}

const (
	PAGE_FILE_NAME = "data.db"
)

func Create(path string) *Pager {
	path = path + "/" + PAGE_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	pager := &Pager{
		file: file,
	}

	pager.cache = lru.NewLRU(util.PAGE_CACHE_CAP)

	// 初始化 meta page
	metaData := pagedata.NewMetaData()
	metaPage := pager.NewPage(metaData)

	pager.Flush(metaPage)
	return pager
}

func Open(path string) *Pager {
	path = path + "/" + PAGE_FILE_NAME
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	pager := &Pager{
		file: file,
	}

	pager.cache = lru.NewLRU(util.PAGE_CACHE_CAP)

	metaPage, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	metaData := metaPage.data.(*pagedata.MetaData)
	if metaData.Version != util.VERSION {
		log.Fatalf("version not match")
	}
	return pager
}

func (pager *Pager) PageFile() *os.File {
	return pager.file
}

func (pager *Pager) SetCacheEviction(eviction cache.Eviction) {
	pager.cache.SetEviction(eviction)
}

// 选择一个具有可用空间的 page
func (pager *Pager) Select(spaceSize uint16, tableName string) (page *Page, err error) {
	if spaceSize > util.PAGE_SIZE {
		return nil, fmt.Errorf("space size %d is too large", spaceSize)
	}

	metaData := pager.GetMetaData()

	table := metaData.GetTableInfo(tableName)
	page, err = pager.GetPage(table.LastPageNum, pagedata.NewRecordData())
	if err != nil {
		err = fmt.Errorf("meta page error, table '%v' last data page not found", table.TableName)
		return
	}

	if util.PAGE_SIZE-uint16(page.Size()-128) >= spaceSize {
		// 如果 page 可用空间大于等于需要的空间，则直接返回
		return page, nil
	} else {
		// 如果 page 可用空间小于需要的空间，则需要分配新的 page
		newDataPage := pager.NewPage(pagedata.NewRecordData())
		newDataPage.nextPageNum = NIL_PAGE_NUM
		newDataPage.prevPageNum = page.pageNum
		page.nextPageNum = newDataPage.pageNum
		table.SetLastPageNum(newDataPage.pageNum)
		return newDataPage, nil
	}
}

func (pager *Pager) NextPageNum(pageNum util.UUID) (util.UUID, error) {
	page, err := pager.GetPage(pageNum, pagedata.NewRecordData())
	if err != nil {
		err = fmt.Errorf("get page failed: %v", err)
		return NIL_PAGE_NUM, err
	}
	return page.nextPageNum, nil
}

func (pager *Pager) NewPage(pageData pagedata.PageData) *Page {
	stat, _ := pager.file.Stat()
	fileSize := stat.Size()

	pageNum := util.UUID(fileSize / util.PAGE_SIZE)
	page := newPage(pageNum, pageData)
	pager.cache.Set(pageNum, page)
	pager.Flush(page)
	return page
}

func (pager *Pager) GetPage(pageNum util.UUID, pageData pagedata.PageData) (*Page, error) {
	if page, hit := pager.cache.Get(pageNum); hit {
		return page.(*Page), nil
	} else {
		pager.file.Seek(0, io.SeekEnd)
		page, err := LoadPage(pager.file, pageData)
		if err != nil {
			log.Fatalf("load page failed: %v", err)
			return nil, err
		}
		pager.cache.Set(pageNum, page)
		return page, nil
	}
}

func (pager *Pager) GetMetaData() *pagedata.MetaData {
	page, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	return page.data.(*pagedata.MetaData)
}

func (pager *Pager) Flush(page *Page) {
	n, err := pager.file.WriteAt(page.Raw(), int64(uint32(page.pageNum)*util.PAGE_SIZE))
	if err != nil || n != util.PAGE_SIZE {
		log.Fatalf("write page %d failed: %v", page.pageNum, err)
	}
	pager.file.Sync()
}

func (pager *Pager) Close() {
	pager.cache.Close()
	pager.file.Close()
}
