package pager

import (
	"minidb-go/util"
	"minidb-go/util/cache"
	"minidb-go/util/lru"
	"os"

	log "github.com/sirupsen/logrus"
)

type PageCache struct {
	cache cache.Cache
	file  *os.File
}

func CreatePageCache(path string) *PageCache {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	cache := lru.NewLRU(16)
	pageCache := newPageCache(cache, file)
	pageCache.cache.SetEviction(func(key, value interface{}) {
		page := value.(*Page)
		pageCache.Flush(page)
	})
	return pageCache
}

func OpenPageCache(path string) *PageCache {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	cache := lru.NewLRU(16)
	pageCache := newPageCache(cache, file)
	pageCache.cache.SetEviction(func(key, value interface{}) {
		page := value.(*Page)
		pageCache.Flush(page)
	})
	return pageCache
}

func newPageCache(cache cache.Cache, file *os.File) *PageCache {
	return &PageCache{
		cache: cache,
		file:  file,
	}
}

func (pageCache *PageCache) NewPage(owner uint16, pageType PageDataType) *Page {
	fileSize, err := pageCache.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalf("seek file failed: %v", err)
	}

	pageNum := util.UUID(fileSize / PageSize)
	page := NewPage(pageNum, pageType)
	pageCache.cache.Set(pageNum, page)
	pageCache.Flush(page)
	return page
}

func (pageCache *PageCache) GetPage(pageNum util.UUID) (*Page, bool) {
	if page, ok := pageCache.cache.Get(pageNum); ok {
		return page.(*Page), true
	} else {
		data := make([]byte, PageSize)
		n, err := pageCache.file.ReadAt(data, int64(pageNum)*PageSize)
		if err != nil || n != PageSize {
			log.Fatalf("read page %d failed: %v", pageNum, err)
		}
		page := LoadPage(pageNum, data)
		pageCache.cache.Set(pageNum, page)
		return page, true
	}
}

func (pageCache *PageCache) Flush(page *Page) {
	n, err := pageCache.file.WriteAt(page.Raw(), int64(uint32(page.pageNum)*PageSize))
	if err != nil || n != PageSize {
		log.Fatalf("write page %d failed: %v", page.pageNum, err)
	}
	pageCache.file.Sync()
}

func (pageCache *PageCache) Close() {
	pageCache.file.Close()
}
