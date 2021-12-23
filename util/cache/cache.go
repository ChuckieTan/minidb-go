package cache

type Cache interface {
	// Get returns the value for the given key.
	Get(key interface{}) (value interface{}, ok bool)

	// Set sets the value for the given key.
	Set(key interface{}, value interface{})

	// Remove deletes the value for the given key.
	Remove(key interface{})

	// Clear clears the cache.
	// Clear()

	// Len returns the number of items in the cache.
	Len() int

	// Cap returns the capacity of the cache.
	// Cap() int

	// Keys returns the keys in the cache.
	// Keys() []string

	// Values returns the values in the cache.
	// Values() [][]byte

	// Items returns the items in the cache.
	// Items() map[string][]byte

	// SetCapacity sets the capacity of the cache.
	// SetCapacity(capacity int)

	// SetEviction sets the eviction function for the cache.
	SetEviction(eviction Eviction)

	// SetExpiration sets the expiration function for the cache.
	// SetExpiration(expiration Expiration)
}

type Eviction func(key, value interface{})

// type Expiration func(key string, value []byte) bool

type CacheItems []*CacheItem

type CacheItem struct {
	Key   interface{}
	Value interface{}
}
