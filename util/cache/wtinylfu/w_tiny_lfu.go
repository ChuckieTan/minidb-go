package wtinylfu

import (
	"container/list"
	"errors"
	"math/rand"
	"sync"
)

type KeyType interface {
	Hashable
	comparable
}

type entry[K KeyType, V any] struct {
	key   K
	value V
}

type Eviction[K KeyType, V any] func(key K, value V)

type WTinyLFU[K KeyType, V any] struct {
	onEvicted Eviction[K, V]

	maxSize int

	// 新放入的元素统一放入 window 中
	windowSize int
	windowList *list.List
	windowMap  map[K]*list.Element

	// probation 存放即将淘汰的数据
	probationSize int
	probationList *list.List
	probationMap  map[K]*list.Element

	// protection 中存放至少被访问两次的数据，这里面的数据暂时不会被删除
	protectionSize int
	protectionList *list.List
	protectionMap  map[K]*list.Element

	// 频率列表
	frequencyTable *CountMinSketch

	lock sync.Mutex
}

// NewWTinyLFU[K KeyType, V any] 初始化一个新的 WTinyLFU[K KeyType, V any]
// maxEntries: 最大容量，需要大于 100
// onEvicted: 当被淘汰的时候调用的函数
func NewWTinyLFU[K KeyType, V any](maxEntries int, onEvicted Eviction[K, V]) *WTinyLFU[K, V] {
	// window 占最大元素数量的百分之 2
	windowSize := maxEntries * 2 / 100
	// probation 队列的最大元素数量占 20%
	probationSize := (maxEntries - windowSize) * 2 / 10
	// protection 队列的最大元素数量占 80%
	protectionSize := maxEntries - windowSize - probationSize
	return &WTinyLFU[K, V]{
		onEvicted: onEvicted,

		maxSize: maxEntries,

		windowSize: windowSize,
		windowList: list.New(),
		windowMap:  make(map[K]*list.Element),

		probationSize: probationSize,
		probationList: list.New(),
		probationMap:  make(map[K]*list.Element),

		protectionSize: protectionSize,
		protectionList: list.New(),
		protectionMap:  make(map[K]*list.Element),

		frequencyTable: NewCountMinSketch(maxEntries),
	}
}

func (cache *WTinyLFU[K, V]) Get(key K) (value V, ok bool) {
	cache.lock.Lock()
	defer func() {
		if ok {
			cache.frequencyTable.Add(key)
			cache.lock.Unlock()
		}
	}()
	// 先看是否在 protection 中
	if elementValue, ok := cache.protectionMap[key]; ok {
		// 把元素移动到 protection 开头
		cache.protectionList.MoveToFront(elementValue)
		// 返回元素的 value
		return elementValue.Value.(*entry[K, V]).value, true
	}
	// 再看是否在 probation 中
	if _, ok := cache.probationMap[key]; ok {
		// 如果在 probation 中，则把对应的元素移动到 protection 中
		entry := cache.moveToProtection(key)
		return entry.value, true
	}
	// 再看是否在 window 中
	if elementValue, ok := cache.windowMap[key]; ok {
		// 把元素移动到 window 开头
		cache.windowList.MoveToFront(elementValue)
		// 返回元素的 value
		return elementValue.Value.(*entry[K, V]).value, true
	}
	return *new(V), false
}

// 将元素从 probation 移动到 protection，并返回元素的 value
func (cache *WTinyLFU[K, V]) moveToProtection(key K) *entry[K, V] {
	if elementValue, ok := cache.probationMap[key]; ok {
		entry := elementValue.Value.(*entry[K, V])
		// 把元素从 probation 中移除
		cache.removeFrom(LRU_PROBATION, key)
		// 把元素放入 protection 中
		cache.insertInto(LRU_PROTECTION, entry)
		cache.adjust()
		return entry
	}
	return nil
}

func (cache *WTinyLFU[K, V]) Set(key K, value V) {
	cache.lock.Lock()
	defer func() {
		cache.frequencyTable.Add(key)
		cache.lock.Unlock()
	}()
	// 先看是否在 protection 中
	if elementValue, ok := cache.protectionMap[key]; ok {
		// 更新元素的 value
		elementValue.Value.(*entry[K, V]).value = value
		// 把元素移动到 protection 开头
		cache.protectionList.MoveToFront(elementValue)
		return
	}
	// 再看是否在 probation 中
	if elementValue, ok := cache.probationMap[key]; ok {
		// 更新元素的 value
		elementValue.Value.(*entry[K, V]).value = value
		// 把元素移动到 protection 中
		cache.moveToProtection(key)
		return
	}
	// 再看是否在 window 中
	if elementValue, ok := cache.windowMap[key]; ok {
		// 更新元素的 value
		elementValue.Value.(*entry[K, V]).value = value
		// 把元素移动到 window 开头
		cache.windowList.MoveToFront(elementValue)
		return
	}
	// 如果不在任何队列中，则添加新元素
	cache.addNewElem(key, value)
}

// 向 window 中添加一个新元素
func (cache *WTinyLFU[K, V]) addNewElem(key K, value V) {
	entry := &entry[K, V]{key, value}
	// 新元素都放入 window 中
	cache.insertInto(LRU_WINDOW, entry)
	cache.adjust()
}

// 调整整个 cache 的大小
func (cache *WTinyLFU[K, V]) adjust() {
	// 如果 window 的大小超过了限制，则移动到 probation 中
	for cache.windowList.Len() > cache.windowSize {
		// 删除 window 的最后一个元素
		last := cache.windowList.Back().Value.(*entry[K, V])
		// 删除 window 中的元素
		cache.removeFrom(LRU_WINDOW, last.key)
		// 把元素放入 probation 中
		cache.insertInto(LRU_PROBATION, last)
	}
	for cache.protectionList.Len() > cache.protectionSize {
		// 删除 protection 的最后一个元素
		last := cache.protectionList.Back().Value.(*entry[K, V])
		// 删除 protection 中的元素
		cache.removeFrom(LRU_PROTECTION, last.key)
		// 把元素放入 probation 中
		cache.insertInto(LRU_PROBATION, last)
	}
	for cache.probationList.Len() > cache.probationSize {
		// 受害者是队尾元素，候选者是队头元素
		victim := cache.probationList.Back()
		candidate := cache.probationList.Front()
		if cache.compete(victim, candidate) {
			// 删除 victim
			entry := victim.Value.(*entry[K, V])
			cache.probationList.Remove(victim)
			delete(cache.probationMap, entry.key)
			cache.onEvicted(entry.key, entry.value)
		} else {
			// 删除 candidate
			entry := candidate.Value.(*entry[K, V])
			cache.probationList.Remove(candidate)
			delete(cache.probationMap, entry.key)
			cache.onEvicted(entry.key, entry.value)
		}
	}
}

// 综合对比两个元素的访问次数，如果删除 victim，则返回 true，反之返回 false
func (cache *WTinyLFU[K, V]) compete(victim *list.Element, candidate *list.Element) bool {
	victimKey := victim.Value.(*entry[K, V]).key
	candidateKey := candidate.Value.(*entry[K, V]).key
	victimFreq := cache.frequencyTable.Count(victimKey)
	candidateFreq := cache.frequencyTable.Count(candidateKey)
	if victimFreq < candidateFreq {
		return true
	} else if candidateFreq <= 5 {
		return false
	}
	// 其他情况随机淘汰
	return rand.Intn(2) == 0
}

type LruType int

const (
	LRU_WINDOW = iota
	LRU_PROBATION
	LRU_PROTECTION
)

func (cache *WTinyLFU[K, V]) getLRU(lru LruType) (*list.List, map[K]*list.Element, error) {
	switch lru {
	case LRU_WINDOW:
		return cache.windowList, cache.windowMap, nil
	case LRU_PROBATION:
		return cache.probationList, cache.probationMap, nil
	case LRU_PROTECTION:
		return cache.protectionList, cache.protectionMap, nil
	default:
		return nil, nil, errors.New("invalid lru type")
	}
}

// 从指定 LRU 里面删除元素
func (cache *WTinyLFU[K, V]) removeFrom(lru LruType, key K) *entry[K, V] {
	list, elemMap, err := cache.getLRU(lru)
	if err != nil {
		return nil
	}
	if elementValue, ok := elemMap[key]; ok {
		// 删除 window 中的元素
		list.Remove(elementValue)
		delete(elemMap, key)
		return elementValue.Value.(*entry[K, V])
	}
	return nil
}

// 向指定 LRU 队头插入元素
func (cache *WTinyLFU[K, V]) insertInto(lru LruType, entry *entry[K, V]) {
	list, elemMap, err := cache.getLRU(lru)
	if err != nil {
		return
	}
	if elementValue, ok := elemMap[entry.key]; ok {
		elementValue.Value = entry
		list.MoveToFront(elementValue)
	} else {
		elem := list.PushFront(entry)
		elemMap[entry.key] = elem
	}
}
