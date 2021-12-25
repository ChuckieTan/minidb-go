package pager

import (
	"math/rand"
	"minidb-go/util"

	log "github.com/sirupsen/logrus"
)

type pageFreeSpace struct {
	pageNum util.UUID
	size    uint16
}

type Pager struct {
	availablePages *[]pageFreeSpace // available pages for each owner
	cache          *PageCache
}

func Create(path string) *Pager {
	pager := &Pager{
		availablePages: nil,
		cache:          CreatePageCache(path),
	}
	metaPage := pager.cache.NewPage(0, META_PAGE)

	// 初始化 meta page
	metaData := metaPage.data.(*MetaData)
	metaData.checksum = rand.Uint32()
	metaData.checksumCopy = 0
	metaData.version = util.VERSION
	metaData.tables = make([]TableInfo, 0)
	metaData.freeList = make([]pageFreeSpace, 0)
	metaData.freeList = append(metaData.freeList, pageFreeSpace{0, util.PageSize})

	// pager 的 availablePages 绑定 meta page
	pager.availablePages = &metaData.freeList

	pager.cache.Flush(metaPage)
	return pager
}

func Open(path string) *Pager {
	pager := &Pager{
		availablePages: nil,
		cache:          OpenPageCache(path),
	}

	metaPage, ok := pager.cache.GetPage(0)
	if !ok {
		log.Fatalf("meta page not found")
	}
	metaData := metaPage.data.(*MetaData)
	if metaData.version != util.VERSION {
		log.Fatalf("version not match")
	}
	if metaData.checksum != metaData.checksumCopy {
		// TODO: checksum 不匹配时，需要重新恢复数据
		log.Fatalf("checksum not match")
	}

	// pager 的 availablePages 绑定 meta page
	pager.availablePages = &metaData.freeList

	return pager
}

// SelectPage returns the page number which has enough free space.
func (pager *Pager) Select(spaceSize uint16, owner uint16) (page *Page, ok bool) {
	if spaceSize > util.PageSize {
		return nil, false
	}

	if (*pager.availablePages)[owner].size >= spaceSize {
		pageNum := (*pager.availablePages)[owner].pageNum
		page, ok = pager.cache.GetPage(pageNum)
		if !ok {
			return nil, false
		}
		(*pager.availablePages)[owner].size -= spaceSize
		return page, true
	}
	var pageType PageType
	if owner == 0 {
		pageType = META_PAGE
	} else {
		pageType = DATA_PAGE
	}
	page = pager.cache.NewPage(owner, pageType)
	(*pager.availablePages)[owner].pageNum = page.pageNum
	(*pager.availablePages)[owner].size = util.PageSize - spaceSize
	return page, true
}
