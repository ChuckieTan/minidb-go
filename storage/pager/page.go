package pager

import (
	"bytes"
	"io"
	"minidb-go/transaction"
	"minidb-go/util"
	"sync"

	log "github.com/sirupsen/logrus"
)

type PageType uint8

const (
	META_PAGE PageType = iota
	DATA_PAGE
	INDEX_PAGE
)

const (
	NIL_PAGE_NUM util.UUID = util.UUID(1<<32 - 1)
)

type Page struct {
	pageNum util.UUID

	nextPageNum util.UUID
	prevPageNum util.UUID

	pageType PageType

	data     PageData
	dataCopy PageData

	dirty bool

	rwlock sync.RWMutex
}

func NewPage(pageNum util.UUID,
	pageType PageType,
	owner uint16) *Page {

	var pageData PageData
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

		nextPageNum: NIL_PAGE_NUM,
		prevPageNum: NIL_PAGE_NUM,

		pageType: pageType,

		data:     pageData,
		dataCopy: pageData,

		dirty: true,
	}
}

func LoadPage(r io.Reader) *Page {
	page := &Page{}
	err := util.Decode(r, &page.pageNum)
	if err != nil {
		log.Fatalf("decode page num failed: %v", err)
	}
	err = util.Decode(r, &page.pageType)
	if err != nil {
		log.Fatalf("decode page type failed: %v", err)
	}
	err = util.Decode(r, &page.nextPageNum)
	if err != nil {
		log.Fatalf("decode next page num failed: %v", err)
	}
	err = util.Decode(r, &page.prevPageNum)
	if err != nil {
		log.Fatalf("decode prev page num failed: %v", err)
	}
	page.data = LoadPageData(r, page.pageType)
	page.dataCopy = page.data
	page.dirty = false
	return page
}

func (p *Page) PageNum() util.UUID {
	return p.pageNum
}

// 返回 Page 数据的二进制
func (page *Page) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, util.PageSize))
	util.Encode(buff, page.pageNum)
	util.Encode(buff, page.pageType)
	util.Encode(buff, page.nextPageNum)
	util.Encode(buff, page.prevPageNum)
	buff.Write(page.data.Raw())
	return buff.Bytes()
}

// 以共享的方式返回 Page 的数据
func (p *Page) Data() *PageData {
	return &p.data
}

func (p *Page) NextPageNum() util.UUID {
	return p.nextPageNum
}

func (p *Page) SetNextPageNum(pageNum util.UUID) {
	p.nextPageNum = pageNum
}

func (p *Page) PrevPageNum() util.UUID {
	return p.nextPageNum
}

func (p *Page) SetPrevPageNum(pageNum util.UUID) {
	p.nextPageNum = pageNum
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
	util.DeepCopy(&p.dataCopy, &p.data)
	return
}

func (p *Page) AfterWrite() {
	p.rwlock.Unlock()
}
