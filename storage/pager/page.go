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

func newPage(pageNum util.UUID,
	pageData PageData,
	owner uint16) *Page {

	return &Page{
		pageNum: pageNum,

		nextPageNum: NIL_PAGE_NUM,
		prevPageNum: NIL_PAGE_NUM,

		data:     pageData,
		dataCopy: pageData,

		dirty: true,
	}
}

func LoadPage(r io.Reader) (*Page, error) {
	page := &Page{}
	err := util.Decode(r, &page.pageNum)
	if err != nil {
		log.Errorf("decode page num failed: %v", err)
		return nil, err
	}
	err = util.Decode(r, &page.pageType)
	if err != nil {
		log.Errorf("decode page type failed: %v", err)
		return nil, err
	}
	err = util.Decode(r, &page.nextPageNum)
	if err != nil {
		log.Errorf("decode next page num failed: %v", err)
		return nil, err
	}
	err = util.Decode(r, &page.prevPageNum)
	if err != nil {
		log.Errorf("decode prev page num failed: %v", err)
		return nil, err
	}
	page.data = LoadPageData(r, page.pageType)
	page.dataCopy = page.data
	page.dirty = false
	return page, nil
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
	dataByte, _ := page.data.GobEncode()
	buff.Write(dataByte)
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

func (p *Page) Size() int {
	return 4 + 4 + 4 + 1 + p.data.Size() + 1
}
