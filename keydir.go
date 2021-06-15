package bitcask

import (
	"sync"
)

type keydir struct {
	mu sync.RWMutex
	m  map[string]*item
}

func NewKeydir() *keydir {
	return &keydir{
		m: make(map[string]*item),
	}
}

func (kd *keydir) Get(key string) (*item, bool) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	item, ok := kd.m[key]
	return item, ok
}

func (kd *keydir) Put(key string, item *item) {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	kd.m[key] = item
}

func (kd *keydir) Delete(key string) {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	delete(kd.m, key)
}

func (kd *keydir) Len() int {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	return len(kd.m)
}
