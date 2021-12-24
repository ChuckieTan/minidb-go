package pager

import (
	"minidb-go/util"
)

const PageSize = 16384 // 16KB

type Pager struct {
	freeSpaces     map[util.UUID]uint16 // free space of each page
	availablePages map[util.UUID]bool   // available pages
	cache          *PageCache
}

func Create(path string) *Pager {
	pager := &Pager{
		freeSpaces: make(map[util.UUID]uint16),
		cache:      CreatePageCache(path),
	}
	return pager
}

func Open(path string) *Pager {
	pager := &Pager{
		freeSpaces: make(map[util.UUID]uint16),
		cache:      OpenPageCache(path)}
	return pager
}

// SelectPage returns the page number which has enough free space.
func (pager *Pager) Select(spaceSize uint16, owner uint16) (page *Page, ok bool) {
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
