package pager

import (
	"bytes"
	"fmt"
	"io"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/storage/recovery/redo/redolog"
	"minidb-go/transaction"
	"minidb-go/util"
	"minidb-go/util/byteconv"
	"sync"
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

// Page 本身不判断是否损坏（即checksum不匹配），判断部分写由 Double Write 完成。
type Page struct {
	pageNum util.UUID

	LSN int64

	nextPageNum util.UUID
	prevPageNum util.UUID

	logs []redolog.Log

	data     pagedata.PageData
	dataCopy pagedata.PageData

	rwlock sync.RWMutex
}

func newPage(pageNum util.UUID, pageData pagedata.PageData) *Page {
	return &Page{
		pageNum: pageNum,

		nextPageNum: NIL_PAGE_NUM,
		prevPageNum: NIL_PAGE_NUM,

		data:     pageData,
		dataCopy: pageData,
	}
}

func LoadPage(r io.Reader, pageData pagedata.PageData) (*Page, error) {
	page := &Page{}

	err := byteconv.Decode(r, &page.pageNum)
	if err != nil {
		err = fmt.Errorf("decode page num failed: %v", err)
		return nil, err
	}
	err = byteconv.Decode(r, &page.LSN)
	if err != nil {
		err = fmt.Errorf("decode LSN failed: %v", err)
		return nil, err
	}
	err = byteconv.Decode(r, &page.nextPageNum)
	if err != nil {
		err = fmt.Errorf("decode next page num failed: %v", err)
		return nil, err
	}
	err = byteconv.Decode(r, &page.prevPageNum)
	if err != nil {
		err = fmt.Errorf("decode prev page num failed: %v", err)
		return nil, err
	}
	var dataLen uint16
	err = byteconv.Decode(r, &dataLen)
	if err != nil {
		err = fmt.Errorf("decode data length failed: %v", err)
		return nil, err
	}
	page.data = pageData
	page.data.Decode(r)
	page.dataCopy = page.data

	if err != nil {
		return nil, err
	}

	return page, nil
}

func (p *Page) PageNum() util.UUID {
	return p.pageNum
}

// 返回 Page 数据的二进制，PageSize的大小
func (page *Page) Raw() []byte {
	buff := bytes.NewBuffer(make([]byte, util.PAGE_SIZE))
	byteconv.Encode(buff, page.pageNum)
	byteconv.Encode(buff, page.LSN)
	byteconv.Encode(buff, page.nextPageNum)
	byteconv.Encode(buff, page.prevPageNum)
	dataByte := page.data.Encode()
	buff.Write(dataByte)
	return buff.Bytes()
}

func (page *Page) Logs() []redolog.Log {
	return page.logs
}

func (page *Page) AppendLog(log redolog.Log) {
	page.logs = append(page.logs, log)
}

// 以共享的方式返回 Page 的数据
func (p *Page) Data() pagedata.PageData {
	return p.data
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
	util.DeepCopy(&p.dataCopy, &p.data)
	return
}

func (p *Page) AfterWrite() {
	p.rwlock.Unlock()
}

func (p *Page) Size() int {
	return 4 + 4 + 4 + 1 + p.data.Size() + 1
}
