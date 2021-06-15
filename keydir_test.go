package bitcask

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeydirGet(t *testing.T) {
	keydir := NewKeydir()
	assert.Equal(t, keydir.Len(), 0)
	key := "key"
	_, ok := keydir.Get(key)
	assert.Equal(t, ok, false)
}

func TestKeydirPut(t *testing.T) {
	keydir := NewKeydir()
	assert.Equal(t, keydir.Len(), 0)
	key := "key"
	item1 := &item{
		fileID:      1,
		valueSize:   2,
		valueOffset: 4,
		timestamp:   8,
	}

	keydir.Put(key, item1)
	assert.Equal(t, keydir.Len(), 1)

	item2, ok := keydir.Get(key)
	assert.Equal(t, ok, true)
	assert.Equal(t, item2, item1)
}

func TestKeydirDelete(t *testing.T) {
	keydir := NewKeydir()
	key := "key"
	item := &item{
		fileID:      1,
		valueSize:   2,
		valueOffset: 4,
		timestamp:   8,
	}

	assert.Equal(t, keydir.Len(), 0)
	keydir.Delete(key)

	keydir.Put(key, item)
	assert.Equal(t, keydir.Len(), 1)

	keydir.Delete(key)
	assert.Equal(t, keydir.Len(), 0)
}

func BenchmarkKeydirGet(b *testing.B) {
	keydir := NewKeydir()

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keydir.Get(keys[i])
	}
}

func BenchmarkKeydirGetParallel(b *testing.B) {
	keydir := NewKeydir()
	key := "key"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keydir.Get(key)
		}
	})
}

func BenchmarkKeydirPut(b *testing.B) {
	keydir := NewKeydir()

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = strconv.Itoa(i)
	}
	item := &item{
		fileID:      1,
		valueSize:   2,
		valueOffset: 4,
		timestamp:   8,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keydir.Put(keys[i], item)
	}
}

func BenchmarkKeydirPutParallel(b *testing.B) {
	keydir := NewKeydir()
	key := "key"
	item := &item{
		fileID:      1,
		valueSize:   2,
		valueOffset: 4,
		timestamp:   8,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keydir.Put(key, item)
		}
	})
}

func BenchmarkKeydirDelete(b *testing.B) {
	keydir := NewKeydir()

	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = strconv.Itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keydir.Delete(keys[i])
	}
}

func BenchmarkKeydirDeleteParallel(b *testing.B) {
	keydir := NewKeydir()
	key := "key"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			keydir.Delete(key)
		}
	})
}

func BenchmarkKeydirLen(b *testing.B) {
	keydir := NewKeydir()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keydir.Len()
	}
}
