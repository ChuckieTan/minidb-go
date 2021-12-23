package pager

import (
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

type Page interface {
	PageNum() util.UUID
	Raw() []byte
	Data() *PageData
	Dirty() bool
	SetDirty()
	BeforeRead() (XID transaction.XID)
	AfterRead()
	BeforeWrite() (XID transaction.XID)
	AfterWrite()
}

type DataPage struct {
	pageNum util.UUID
	owner   uint16

	data     *PageData
	dataCopy *PageData

	dirty bool

	rwlock sync.RWMutex
}

func NewPage(pageNum util.UUID, pageType PageType) *DataPage {
	panic("page data type is not supported")
	return &DataPage{
		pageNum: pageNum,
		owner:   0,
		dirty:   true,
	}
}

func LoadPage(pageNum util.UUID, data []byte) *DataPage {
	panic("implement me")
	pageData := LoadPageData(data)
	return &DataPage{
		pageNum:  pageNum,
		owner:    1,
		data:     pageData,
		dataCopy: pageData,
		dirty:    false,
	}
}

func (p *DataPage) PageNum() util.UUID {
	return p.pageNum
}

func (p *DataPage) Raw() []byte {
	panic("implement me")
}

// 以共享的方式返回 Page 的数据
func (p *DataPage) Data() *PageData {
	return p.data
}

func (p *DataPage) Dirty() bool {
	return p.dirty
}

func (p *DataPage) SetDirty() {
	p.dirty = true
}

func (p *DataPage) BeforeRead() (XID transaction.XID) {
	p.rwlock.RLock()
	util.DeepCopy(&p.dataCopy, &p.data)
	return
}

func (p *DataPage) AfterRead() {
	p.rwlock.RUnlock()
}

func (p *DataPage) BeforeWrite() (XID transaction.XID) {
	p.rwlock.Lock()
	p.SetDirty()
	util.DeepCopy(p.dataCopy, p.data)
	return
}

func (p *DataPage) AfterWrite() {
	p.rwlock.Unlock()
}
