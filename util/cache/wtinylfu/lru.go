package wtinylfu

// import (
// 	"container/list"
// 	"sync"

// 	log "github.com/sirupsen/logrus"
// )

// type LRU[K comparable, V any] struct {
// 	MaxEntries int
// 	OnEvicted  func(key K, value V)

// 	// list 存放 entry 的指针
// 	cacheList *list.List
// 	// map 存放 list node 的指针
// 	cacheMap map[K]*list.Element

// 	lock sync.RWMutex
// }

// func NewLRU[K comparable, V any](maxEntries int, onEvicted Eviction[K, V]) *LRU[K, V] {
// 	return &LRU[K, V]{
// 		MaxEntries: maxEntries,
// 		cacheList:  list.New(),
// 		cacheMap:   make(map[K]*list.Element),
// 		OnEvicted:  onEvicted,
// 	}
// }

// func (cache *LRU[K, V]) Set(key K, value V) {
// 	cache.lock.Lock()
// 	defer cache.lock.Unlock()
// 	if cache.cacheList == nil {
// 		cache.cacheList = list.New()
// 		cache.cacheMap = make(map[K]*list.Element)
// 	}

// 	if elementValue, ok := cache.cacheMap[key]; ok {
// 		cache.cacheList.MoveToFront(elementValue)
// 		elementValue.Value = &entry[K, V]{key, value}
// 		return
// 	}

// 	element := cache.cacheList.PushFront(&entry[K, V]{key, value})
// 	cache.cacheMap[key] = element

// 	if cache.MaxEntries != 0 && cache.cacheList.Len() > cache.MaxEntries {
// 		cache.removeOldest()
// 	}
// }

// func (cache *LRU[K, V]) Get(key K) (value V, ok bool) {
// 	cache.lock.RLock()
// 	defer cache.lock.RUnlock()

// 	if cache.cacheMap == nil {
// 		return
// 	}

// 	// 返回 value 的指针，使之可以修改
// 	if ele, hit := cache.cacheMap[key]; hit {
// 		cache.cacheList.MoveToFront(ele)
// 		return ele.Value.(*entry[K, V]).value, true
// 	}
// 	return
// }

// func (cache *LRU[K, V]) removeOldest() {
// 	if cache.cacheMap == nil {
// 		return
// 	}
// 	element := cache.cacheList.Back()
// 	if element != nil {
// 		cache.removeElement(element)
// 	}
// }

// func (cache *LRU[K, V]) Remove(key K) {
// 	cache.lock.Lock()
// 	defer cache.lock.Unlock()
// 	if cache.cacheMap == nil {
// 		return
// 	}
// 	if ele, hit := cache.cacheMap[key]; hit {
// 		cache.removeElement(ele)
// 	}
// }

// func (cache *LRU[K, V]) removeElement(element *list.Element) {
// 	node := element.Value.(*entry[K, V])
// 	key, value := node.key, node.value
// 	delete(cache.cacheMap, key)
// 	if cache.OnEvicted != nil {
// 		cache.OnEvicted(key, value)
// 	}
// 	cache.cacheList.Remove(element)
// }

// func (cache *LRU[K, V]) Len() int {
// 	cache.lock.RLock()
// 	defer cache.lock.RUnlock()

// 	return cache.cacheList.Len()
// }

// func (cache *LRU[K, V]) SetEviction(eviction Eviction[K, V]) {
// 	cache.OnEvicted = eviction
// }

// func (cache *LRU[K, V]) Close() {
// 	log.Info("clearing pages in cache...")
// 	if cache.OnEvicted != nil {
// 		for cache.cacheList.Len() > 0 {
// 			entry := cache.cacheList.Back().Value.(*entry[K, V])
// 			cache.OnEvicted(entry.key, entry.value)
// 			cache.cacheList.Remove(cache.cacheList.Back())
// 			delete(cache.cacheMap, entry.key)
// 		}
// 	}
// }
