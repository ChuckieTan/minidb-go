package pager

import (
	"bytes"
	"encoding/binary"
	"io"
	"minidb-go/storage/pager/pagedata"
	"minidb-go/storage/recovery/redo/redolog"
	"minidb-go/util"
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

	data pagedata.PageData

	rwlock sync.RWMutex
}

func newPage(pageNum util.UUID, pageData pagedata.PageData) *Page {
	return &Page{
		pageNum: pageNum,

		nextPageNum: NIL_PAGE_NUM,
		prevPageNum: NIL_PAGE_NUM,

		logs: make([]redolog.Log, 0),

		data: pageData,
	}
}

func LoadPage(r io.Reader, pageData pagedata.PageData) (*Page, error) {
	page := &Page{}

	binary.Read(r, binary.BigEndian, &page.pageNum)
	binary.Read(r, binary.BigEndian, &page.LSN)
	binary.Read(r, binary.BigEndian, &page.nextPageNum)
	binary.Read(r, binary.BigEndian, &page.prevPageNum)
	page.data = pageData
	page.data.Decode(r)
	return page, nil
}

func (p *Page) PageNum() util.UUID {
	return p.pageNum
}

// 返回 Page 数据的二进制，PageSize的大小
func (page *Page) Raw() []byte {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, page.pageNum)
	binary.Write(buff, binary.BigEndian, page.LSN)
	binary.Write(buff, binary.BigEndian, page.nextPageNum)
	binary.Write(buff, binary.BigEndian, page.prevPageNum)
	dataByte := page.data.Encode()
	buff.Write(dataByte)
	zeroLen := util.PAGE_SIZE - buff.Len()
	buff.Write(make([]byte, zeroLen))
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

func (p *Page) Size() int {
	return 4 + 8 + 4 + 4 + p.data.Size()
}
