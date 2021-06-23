package bitcask

import (
	"hash/fnv"
	"sync"
)

const n = 512

type shard struct {
	mu sync.RWMutex
	m  map[string]*item
}

type keydir struct {
	shards [n]*shard
}

func NewKeydir() *keydir {
	kd := new(keydir)
	for i := 0; i < n; i++ {
		kd.shards[i] = &shard{
			m: make(map[string]*item),
		}
	}
	return kd
}

func (kd *keydir) shard(key string) *shard {
	h := fnv.New64()
	h.Write([]byte(key))
	return kd.shards[h.Sum64()%n]
}

func (kd *keydir) Get(key string) (*item, bool) {
	shard := kd.shard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	item, ok := shard.m[key]
	return item, ok
}

func (kd *keydir) Put(key string, item *item) {
	shard := kd.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.m[key] = item
}

func (kd *keydir) Delete(key string) {
	shard := kd.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.m, key)
}

// TODO: performance optimization
func (kd *keydir) Len() int {
	l := 0
	for i := 0; i < n; i++ {
		shard := kd.shards[i]
		shard.mu.RLock()
		l += len(kd.shards[i].m)
		shard.mu.RUnlock()
	}
	return l
}
