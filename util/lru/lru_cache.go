package lru

import (
	"container/list"
)

type Cache struct {
	MaxEntries int
	OnEvicted  func(key uint32, value interface{})

	// list 存放 entry 的指针
	cacheList *list.List
	// map 存放 list node 的指针
	cacheMap map[interface{}]*list.Element
}

type entry struct {
	key   uint32
	value interface{}
}

func NewLRU(maxEntries int) (cache *Cache) {
	return &Cache{
		MaxEntries: maxEntries,
		cacheList:  list.New(),
		cacheMap:   make(map[interface{}]*list.Element),
	}
}

func (cache *Cache) Add(key uint32, value interface{}) {
	if cache.cacheList == nil {
		cache.cacheList = list.New()
		cache.cacheMap = make(map[interface{}]*list.Element)
	}

	if elementValue, ok := cache.cacheMap[key]; ok {
		cache.cacheList.MoveToFront(elementValue)
		elementValue.Value = value
		return
	}

	element := cache.cacheList.PushFront(&entry{key, value})
	cache.cacheMap[key] = element

	if cache.MaxEntries != 0 && cache.Len() > cache.MaxEntries {
		cache.removeOldest()
	}
}

func (cache *Cache) Get(key uint32) (value interface{}, ok bool) {
	if cache.cacheMap == nil {
		return
	}

	// 返回 value 的指针，使之可以修改
	if ele, hit := cache.cacheMap[key]; hit {
		cache.cacheList.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

func (cache *Cache) removeOldest() {
	if cache.cacheMap == nil {
		return
	}
	element := cache.cacheList.Back()
	if element != nil {
		cache.removeElement(element)
	}
}

func (cache *Cache) Remove(key uint32) {
	if cache.cacheMap == nil {
		return
	}
	if ele, hit := cache.cacheMap[key]; hit {
		cache.removeElement(ele)
	}
}

func (cache *Cache) removeElement(element *list.Element) {
	node := element.Value.(*entry)
	key, value := node.key, node.value
	delete(cache.cacheMap, key)
	if cache.OnEvicted != nil {
		cache.OnEvicted(key, value)
	}
	cache.cacheList.Remove(element)
}

func (cache *Cache) Len() int {
	return cache.cacheList.Len()
}
