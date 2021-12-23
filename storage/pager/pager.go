package pager

import (
	"minidb-go/util"
)

const PageSize = 16384

type Pager struct {
	freeSpace map[util.UUID]uint16 // free space of each page
	pageCache *PageCache
}

func Create(path string) *Pager {
	pager := &Pager{
		freeSpace: make(map[util.UUID]uint16),
		pageCache: CreatePageCache(path),
	}
	return pager
}

func Open(path string) *Pager {
	pager := &Pager{
		freeSpace: make(map[util.UUID]uint16),
		pageCache: OpenPageCache(path)}
	return pager
}

// SelectPage returns the page number which has enough free space.
func (pager *Pager) Select(spaceSize uint16, owner uint16) (page *DataPage, ok bool) {
	for key, value := range pager.freeSpace {
		if value >= spaceSize {
			page, ok = pager.pageCache.GetPage(key)
			return
		}
	}
	var pageType PageDataType
	if owner == 0 {
		pageType = META_PAGE
	} else {
		pageType = DATA_PAGE
	}
	page = pager.pageCache.NewPage(owner, pageType)
	return page, true
}
