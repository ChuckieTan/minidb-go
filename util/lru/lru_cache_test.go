package lru_test

import (
	"minidb-go/util/lru"
	"testing"
)

func TestCache(t *testing.T) {
	cache := lru.NewLRU(2)
	cache.OnEvicted = func(key interface{}, value interface{}) {
		t.Log("remove: ", key, value)
	}
	type P struct {
		X, Y int
	}
	cache.Set(1, &P{1, 2})

	ele, _ := cache.Get(1)
	v := ele.(*P)
	*v = P{2, 3}

	cache.Set(2, &P{2, 3})
	cache.Set(3, &P{3, 4})
}
