package pager

import (
	"io"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/util"
	"minidb-go/util/cache"
	"minidb-go/util/lru"
	"os"

	log "github.com/sirupsen/logrus"
)

type Pager struct {
	cache cache.Cache
	file  *os.File
}

func Create(path string) *Pager {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	pager := &Pager{
		file: file,
	}

	pager.cache = lru.NewLRU(16)
	pager.cache.SetEviction(func(key, value interface{}) {
		page := value.(*Page)
		pager.Flush(page)
	})
	// 初始化 meta page
	metaData := pagedata.NewMetaData()
	metaPage := pager.NewPage(metaData, 0)

	pager.Flush(metaPage)
	return pager
}

func Open(path string) *Pager {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	pager := &Pager{
		file: file,
	}

	pager.cache = lru.NewLRU(16)
	pager.cache.SetEviction(func(key, value interface{}) {
		page := value.(*Page)
		pager.Flush(page)
	})

	metaPage, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	metaData := metaPage.data.(*pagedata.MetaData)
	if metaData.Version() != util.VERSION {
		log.Fatalf("version not match")
	}
	if !metaData.Valid() {
		// TODO: checksum 不匹配时，需要重新恢复数据
		log.Fatalf("checksum not match")
	}

	return pager
}

// 选择一个具有可用空间的 page
func (pager *Pager) Select(spaceSize uint16, owner uint16) (page *Page, ok bool) {
	if spaceSize > util.PAGE_SIZE {
		return nil, false
	}

	metaPage, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	metaData := metaPage.data.(*pagedata.MetaData)

	table := metaData.GetTableInfoByTableId(owner)
	page, err = pager.GetPage(table.LastPageNum(), pagedata.NewRecordData())
	if err != nil {
		log.Fatalf("meta page error, table '%v' last data page not found", table.TableName())
	}

	if uint16(page.Size()) >= spaceSize {
		return page, true
	}
	// TODO: 新建 page 需要上层模块来完成
	// newDataPage := pager.NewPage(NewRecordData(), owner)
	// newDataPage.nextPageNum = NIL_PAGE_NUM
	// newDataPage.prevPageNum = page.pageNum
	// page.nextPageNum = newDataPage.pageNum
	// metaData.tables[owner-1].lastPageNum = page.pageNum
	return page, false
}

func (pager *Pager) NewPage(pageData pagedata.PageData, owner uint16) *Page {
	fileSize, err := pager.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalf("seek file failed: %v", err)
	}

	pageNum := util.UUID(fileSize / util.PAGE_SIZE)
	page := newPage(pageNum, pageData, owner)
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
			log.Errorf("load page failed: %v", err)
			return nil, err
		}
		pager.cache.Set(pageNum, page)
		return page, nil
	}
}

func (pager *Pager) GetMetaPage() *Page {
	page, err := pager.GetPage(0, pagedata.NewMetaData())
	if err != nil {
		log.Fatalf("get meta page failed: %v", err)
	}
	return page
}

func (pager *Pager) Flush(page *Page) {
	n, err := pager.file.WriteAt(page.Raw(), int64(uint32(page.pageNum)*util.PAGE_SIZE))
	if err != nil || n != util.PAGE_SIZE {
		log.Fatalf("write page %d failed: %v", page.pageNum, err)
	}
	pager.file.Sync()
}

func (pager *Pager) Close() {
	pager.file.Close()
}
