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

// pager 为单例模式，获取唯一的 pager
func GetInstance() *Pager {
	if p == nil {
		p = &Pager{cache: lru.NewLRU(5)}
		p.cache.OnEvicted = p.dump
	}
	return p
}

// 将页序列化到磁盘上，cache 的回调函数
func (pager *Pager) dump(key uint32, value interface{}) {

}

// 返回页号对应的 page
func (pager *Pager) GetPage(pageNumber uint32) (page interface{}, err error) {
	page, _ = pager.cache.Get(pageNumber)
	return
}

// 磁盘存储暂未实现
// 虚拟页号，依次递增
var a uint32 = 1

// 传入一个指针，返回其对应的磁盘页号
func (pager *Pager) NewPage(data interface{}) (addr uint32) {
	addr = a
	a++
	pager.cache.Add(addr, data)
	return
}
