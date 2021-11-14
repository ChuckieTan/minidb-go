package pager

import (
	"io"
	"minidb-go/util/lru"
)

const PageSize = 16384

type Pager struct {
	File  io.ReadWriter
	cache *lru.Cache
}

var p *Pager

func GetInstance() *Pager {
	if p == nil {
		p = &Pager{cache: lru.NewLRU(5)}
		p.cache.OnEvicted = p.dump
	}
	return p
}

func (pager *Pager) dump(key uint32, value interface{}) {

}

func (pager *Pager) GetPage(pageNumber uint32) (page interface{}, err error) {
	page, _ = pager.cache.Get(pageNumber)
	return
}

var a uint32 = 1

func (pager *Pager) NewPage(data interface{}) (addr uint32) {
	addr = a
	a++
	pager.cache.Add(addr, data)
	return
}
