package wtinylfu_test

import (
	"container/list"
	"math"
	"minidb-go/util/cache"
	"sync"
	"testing"
)

type LRU[K comparable, V any] struct {
	MaxEntries int
	OnEvicted  func(key K, value V)

	// list 存放 entry 的指针
	cacheList *list.List
	// map 存放 list node 的指针
	cacheMap map[K]*list.Element

	lock sync.RWMutex
}

func NewLRU[K comparable, V any](maxEntries int, onEvicted cache.Eviction) *LRU[K, V] {
	return &LRU[K, V]{
		MaxEntries: maxEntries,
		cacheList:  list.New(),
		cacheMap:   make(map[K]*list.Element),
		// OnEvicted:  onEvicted,
	}
}

func TestSLRU(t *testing.T) {
	// slru := slru.SLRU[int, int]{1, 2}
	t.Errorf("%T\n", NewLRU[int, int])
	ln2 := float64(math.Log(2))
	tableSize := int(-float64(1000000)*math.Log(0.01)/(ln2*ln2)) / 2
	t.Errorf("%v\n", tableSize)
}
