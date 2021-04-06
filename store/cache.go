package store

import (
	lru "github.com/hashicorp/golang-lru"
)

type Cache interface {
	Get(key string) (value interface{}, ok bool)
	Add(key string, value interface{})
	Remove(key string)
	Keys() []string
	Size() int
}

type MemTableCache struct {
	m map[string]interface{}
}

func (t *MemTableCache) Add(key string, value interface{}) {
	t.m[key] = value
}

func (t *MemTableCache) Get(key string) (value interface{}, ok bool) {
	value, ok = t.m[key]
	return value, ok
}

func (t *MemTableCache) Remove(key string) {
	delete(t.m, key)
}

func (t *MemTableCache) Keys() []string {
	keys := make([]string, 0, len(t.m))
	for k := range t.m {
		keys = append(keys, k)
	}

	return keys
}

func (t *MemTableCache) Size() int {
	return len(t.m)
}

func NewMemTableCache() Cache {
	m := make(map[string]interface{})
	return &MemTableCache{m}
}

type LruCache struct {
	Lru *lru.ARCCache
}

func (l *LruCache) Add(key string, value interface{}) {
	l.Lru.Add(key, value)
}

func (l *LruCache) Get(key string) (value interface{}, ok bool) {
	value, ok = l.Lru.Get(key)
	return value, ok
}

func (l *LruCache) Remove(key string) {
	l.Lru.Remove(key)
}

func (l *LruCache) Keys() []string {
	return l.Keys()
}

func (l *LruCache) Size() int {
	return l.Lru.Len()
}

func NewLruCache() (Cache, error) {
	var cache *lru.ARCCache
	cache, err := lru.NewARC(1000)
	return &LruCache{cache}, err
}
