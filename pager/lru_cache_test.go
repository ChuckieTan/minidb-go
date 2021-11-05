package pager_test

import (
	"minidb-go/pager"
	"testing"
)

func TestCache(t *testing.T) {
	cache := pager.NewLRU(2)
	cache.OnEvicted = func(key pager.Key, value interface{}) {
		t.Log("remove: ", key, value)
	}
	type P struct {
		X, Y int
	}
	cache.Add(1, &P{1, 2})

	ele, _ := cache.Get(1)
	v := ele.(*P)
	*v = P{2, 3}

	cache.Add(2, &P{2, 3})
	cache.Add(3, &P{3, 4})
}
