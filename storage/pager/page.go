package pager

import (
	"io"
	"minidb-go/transaction"
	"minidb-go/util"
	"sync"
)

type PageType uint8

const (
	META_PAGE PageType = iota
	DATA_PAGE
	INDEX_PAGE
)

type Page struct {
	pageNum util.UUID
	owner   uint16

	nextPageNum util.UUID
	prevPageNum util.UUID

	pageType PageType

	data     *PageData
	dataCopy *PageData

	dirty bool

	rwlock sync.RWMutex
}

func NewPage(pageNum util.UUID,
	pageType PageType,
	owner uint16,
	nextPageNum util.UUID,
	prevPageNum util.UUID) *Page {

	var pageData *PageData
	var pageDataType PageDataType
	switch pageType {
	case META_PAGE:
		pageDataType = META_DATA
	case DATA_PAGE:
		pageDataType = RECORE_DATA
	case INDEX_PAGE:
		pageDataType = INDEX_DATA
	default:
		panic("page data type is not supported")
	}
	pageData = NewPageData(pageDataType)

	return &Page{
		pageNum: pageNum,
		owner:   owner,

		nextPageNum: nextPageNum,
		prevPageNum: prevPageNum,

		pageType: pageType,

		data:     pageData,
		dataCopy: pageData,

		dirty: true,
	}
}

func LoadPage(r io.Reader) *Page {
	panic("implement me")
	page := &Page{}
	util.Decode(r, &page.pageNum)
	util.Decode(r, &page.pageType)
	util.Decode(r, &page.owner)
	util.Decode(r, &page.nextPageNum)
	util.Decode(r, &page.prevPageNum)
	page.data = LoadPageData(r)
	page.dataCopy = page.data
	page.dirty = false
	return page
}

func (p *Page) PageNum() util.UUID {
	return p.pageNum
}

func (p *Page) Raw() []byte {
	panic("implement me")
}

// 以共享的方式返回 Page 的数据
func (p *Page) Data() *PageData {
	return p.data
}

func (p *Page) Dirty() bool {
	return p.dirty
}

func (p *Page) SetDirty() {
	p.dirty = true
}

func (p *Page) BeforeRead() (XID transaction.XID) {
	p.rwlock.RLock()
	util.DeepCopy(&p.dataCopy, &p.data)
	return
}

func (p *Page) AfterRead() {
	p.rwlock.RUnlock()
}

func (p *Page) BeforeWrite() (XID transaction.XID) {
	p.rwlock.Lock()
	p.SetDirty()
	util.DeepCopy(p.dataCopy, p.data)
	return
}

func (p *Page) AfterWrite() {
	p.rwlock.Unlock()
}
