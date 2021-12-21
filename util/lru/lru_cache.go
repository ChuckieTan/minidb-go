package lru

import (
	"container/list"
)

type LRU struct {
	MaxEntries int
	OnEvicted  func(key interface{}, value interface{})

	// list 存放 entry 的指针
	cacheList *list.List
	// map 存放 list node 的指针
	cacheMap map[interface{}]*list.Element
}

type entry struct {
	key   interface{}
	value interface{}
}

func NewLRU(maxEntries int) (cache *LRU) {
	return &LRU{
		MaxEntries: maxEntries,
		cacheList:  list.New(),
		cacheMap:   make(map[interface{}]*list.Element),
	}
}

func (cache *LRU) Set(key interface{}, value interface{}) {
	if cache.cacheList == nil {
		cache.cacheList = list.New()
		cache.cacheMap = make(map[interface{}]*list.Element)
	}

	if elementValue, ok := cache.cacheMap[key]; ok {
		cache.cacheList.MoveToFront(elementValue)
		elementValue.Value = &entry{key, value}
		return
	}

	element := cache.cacheList.PushFront(&entry{key, value})
	cache.cacheMap[key] = element

	if cache.MaxEntries != 0 && cache.Len() > cache.MaxEntries {
		cache.removeOldest()
	}
}

func (cache *LRU) Get(key interface{}) (value interface{}, ok bool) {
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

func (cache *LRU) removeOldest() {
	if cache.cacheMap == nil {
		return
	}
	element := cache.cacheList.Back()
	if element != nil {
		cache.removeElement(element)
	}
}

func (cache *LRU) Remove(key interface{}) {
	if cache.cacheMap == nil {
		return
	}
	if ele, hit := cache.cacheMap[key]; hit {
		cache.removeElement(ele)
	}
}

func (cache *LRU) removeElement(element *list.Element) {
	node := element.Value.(*entry)
	key, value := node.key, node.value
	delete(cache.cacheMap, key)
	if cache.OnEvicted != nil {
		cache.OnEvicted(key, value)
	}
	cache.cacheList.Remove(element)
}

func (cache *LRU) Len() int {
	return cache.cacheList.Len()
}

func (cache *LRU) SetEviction(eviction func(key interface{}, value interface{})) {
	cache.OnEvicted = eviction
}
