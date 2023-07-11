package main

import (
	"fmt"

	"k8s.io/client-go/tools/cache"
)

type resourceCache struct {
	// cacheStorage bears the burden of thread safety for the cache
	cacheStorage cache.ThreadSafeStore
	// keyFunc is used to make the key for objects stored in and retrieved from items, and
	// should be deterministic.
	keyFunc cache.KeyFunc
}

var _ cache.Store = &resourceCache{}

// Add inserts an item into the cache.
func (c *resourceCache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return cache.KeyError{Obj: obj, Err: err}
	}
	fmt.Printf("add obj: %v", obj)
	c.cacheStorage.Add(key, obj)
	return nil
}

// Update sets an item in the cache to its updated state.
func (c *resourceCache) Update(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return cache.KeyError{Obj: obj, Err: err}
	}
	fmt.Printf("update obj: %v", obj)
	c.cacheStorage.Update(key, obj)
	return nil
}

// Delete removes an item from the cache.
func (c *resourceCache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return cache.KeyError{Obj: obj, Err: err}
	}
	c.cacheStorage.Delete(key)
	return nil
}

// List returns a list of all the items.
// List is completely threadsafe as long as you treat all items as immutable.
func (c *resourceCache) List() []interface{} {
	return c.cacheStorage.List()
}

// ListKeys returns a list of all the keys of the objects currently
// in the cache.
func (c *resourceCache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

// GetIndexers returns the indexers of cache
func (c *resourceCache) GetIndexers() cache.Indexers {
	return c.cacheStorage.GetIndexers()
}

// Index returns a list of items that match on the index function
// Index is thread-safe so long as you treat all items as immutable
func (c *resourceCache) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return c.cacheStorage.Index(indexName, obj)
}

func (c *resourceCache) IndexKeys(indexName, indexKey string) ([]string, error) {
	return c.cacheStorage.IndexKeys(indexName, indexKey)
}

// ListIndexFuncValues returns the list of generated values of an Index func
func (c *resourceCache) ListIndexFuncValues(indexName string) []string {
	return c.cacheStorage.ListIndexFuncValues(indexName)
}

func (c *resourceCache) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	return c.cacheStorage.ByIndex(indexName, indexKey)
}

func (c *resourceCache) AddIndexers(newIndexers cache.Indexers) error {
	return c.cacheStorage.AddIndexers(newIndexers)
}

// Get returns the requested item, or sets exists=false.
// Get is completely threadsafe as long as you treat all items as immutable.
func (c *resourceCache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, cache.KeyError{Obj: obj, Err: err}
	}
	return c.GetByKey(key)
}

// GetByKey returns the request item, or exists=false.
// GetByKey is completely threadsafe as long as you treat all items as immutable.
func (c *resourceCache) GetByKey(key string) (item interface{}, exists bool, err error) {
	item, exists = c.cacheStorage.Get(key)
	return item, exists, nil
}

// Replace will delete the contents of 'c', using instead the given list.
// 'c' takes ownership of the list, you should not reference the list again
// after calling this function.
func (c *resourceCache) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return cache.KeyError{Obj: item, Err: err}
		}
		items[key] = item
	}
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync is meaningless for one of these
func (c *resourceCache) Resync() error {
	return nil
}

// NewIndexer returns an Indexer implemented simply with a map and a lock.
func NewIndexer(keyFunc cache.KeyFunc, indexers cache.Indexers) cache.Indexer {
	return &resourceCache{
		cacheStorage: cache.NewThreadSafeStore(indexers, cache.Indices{}),
		keyFunc:      keyFunc,
	}
}
