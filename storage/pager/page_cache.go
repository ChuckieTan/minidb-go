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
	return newPageCache(lru.NewLRU(16), file)
}

func OpenPageCache(path string) *PageCache {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("open file %s failed: %v", path, err)
	}
	return newPageCache(lru.NewLRU(16), file)
}

func newPageCache(cache cache.Cache, file *os.File) *PageCache {
	return &PageCache{
		cache: cache,
		file:  file,
	}
}

func (pageCache *PageCache) NewPage() *Page {
	return &Page{}
}

func (pageCache *PageCache) GetPage(pageNum util.UUID) (*Page, bool) {
	if page, ok := pageCache.cache.Get(pageNum); ok {
		return page.(*Page), true
	}
	return &Page{}, true
}
