package bitcask

import (
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	dir = os.TempDir() + "bitcask"
)

func syncMapLen(m *sync.Map) int {
	len := 0
	m.Range(func(key, value interface{}) bool {
		len += 1
		return true
	})
	return len
}

func TestOpenEmptyDir(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Nil(t, err)
	assert.NotNil(t, bitcask)

	assert.EqualValues(t, syncMapLen(bitcask.rfiles), 0)
	assert.EqualValues(t, bitcask.keydir.Len(), 0)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.fileID, 1)
	assert.EqualValues(t, bitcask.offset, 0)
}

func TestOpenOneFileDir(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Nil(t, err)
	assert.NotNil(t, bitcask)

	assert.EqualValues(t, syncMapLen(bitcask.rfiles), 0)
	assert.EqualValues(t, bitcask.keydir.Len(), 0)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.fileID, 1)
	assert.EqualValues(t, bitcask.offset, 0)

	// put
	n := 128
	for i := 0; i < n; i++ {
		key := strconv.Itoa(i)
		value := key
		err = bitcask.Put([]byte(key), []byte(value))
		assert.Nil(t, err)
	}

	err = bitcask.Close()
	assert.Nil(t, err)

	// open
	bitcask, err = Open(dir)
	assert.Nil(t, err)
	assert.NotNil(t, bitcask)

	assert.EqualValues(t, syncMapLen(bitcask.rfiles), 1)
	assert.Equal(t, bitcask.keydir.Len(), n)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.fileID, 1)

	for i := 0; i < n; i++ {
		key := strconv.Itoa(i)
		value, err := bitcask.Get([]byte(key))
		assert.Nil(t, err)
		assert.Equal(t, key, string(value))
	}

	// delete
	m := 64
	for i := 0; i < m; i++ {
		key := strconv.Itoa(i)
		err := bitcask.Delete([]byte(key))
		assert.Nil(t, err)
	}

	err = bitcask.Close()
	assert.Nil(t, err)

	// open
	bitcask, err = Open(dir)
	assert.Nil(t, err)
	assert.NotNil(t, bitcask)

	assert.EqualValues(t, syncMapLen(bitcask.rfiles), 1)
	assert.Equal(t, bitcask.keydir.Len(), n-m)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.fileID, 1)
	for i := m; i < n; i++ {
		key := strconv.Itoa(i)
		value, err := bitcask.Get([]byte(key))
		assert.Nil(t, err)
		assert.Equal(t, key, string(value))
	}

}

func TestDataFilepath(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Nil(t, err)

	assert.Equal(t, dataFilepath(bitcask.dir, 1024), dir+"/bitcask.data.1024")
	// t.Logf("data filename: %s", dataFilepath(bitcask.dir, 1024))
}

func TestPut1(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Equal(t, err, nil)

	key := []byte("key")
	value := []byte("value")

	err = bitcask.Put(key, value)
	assert.Equal(t, err, nil)
	assert.EqualValues(t, bitcask.fileID, 1)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.offset, 16+len(key)+len(value))

	v, err := bitcask.Get(key)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, value)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 1)
}

func TestPut2(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir, WithMaxFileSize(32))
	assert.Equal(t, err, nil)

	key := []byte("key")
	value := []byte("value")

	// put key value
	err = bitcask.Put(key, value)
	assert.Equal(t, err, nil)
	assert.EqualValues(t, bitcask.fileID, 1)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.offset, 16+len(key)+len(value))

	v, err := bitcask.Get(key)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, value)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 1)

	// put key value
	err = bitcask.Put(key, value)
	assert.Equal(t, err, nil)
	assert.EqualValues(t, bitcask.fileID, 2)
	assert.NotNil(t, bitcask.file)
	assert.EqualValues(t, bitcask.offset, 16+len(key)+len(value))

	v, err = bitcask.Get(key)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, value)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 2)
}

func TestGetMiss(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Equal(t, err, nil)

	key := []byte("key")
	v, err := bitcask.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, v)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 0)
}

func TestGetHit(t *testing.T) {
	//defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Equal(t, err, nil)

	key := []byte("key")
	value := []byte("value")

	v, err := bitcask.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, v)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 0)

	err = bitcask.Put(key, value)
	assert.Equal(t, err, nil)

	v, err = bitcask.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, v, value)
	assert.Equal(t, syncMapLen(bitcask.rfiles), 1)
}

func TestDelete(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Equal(t, err, nil)

	key := []byte("key")
	value := []byte("value")

	err = bitcask.Put(key, value)
	assert.Nil(t, err)

	v, err := bitcask.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, v, value)

	err = bitcask.Delete(key)
	assert.Nil(t, err)

	v, err = bitcask.Get(key)
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func TestLen(t *testing.T) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	assert.Equal(t, err, nil)

	n := 1024
	for i := 0; i < n; i++ {
		key := strconv.Itoa(i)
		value := key
		err = bitcask.Put([]byte(key), []byte(value))
		assert.Nil(t, err)
	}
	assert.Equal(t, bitcask.Len(), n)
}

func BenchmarkPut(b *testing.B) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	if err != nil {
		panic(err)
	}

	key := []byte("key")
	value := []byte("value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bitcask.Put(key, value); err != nil {
			panic(err)
		}
	}
}

func BenchmarkPutParallel(b *testing.B) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	if err != nil {
		panic(err)
	}

	key := []byte("key")
	value := []byte("value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bitcask.Put(key, value); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkGet(b *testing.B) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	if err != nil {
		panic(err)
	}

	key := []byte("key")
	value := []byte("value")

	if err := bitcask.Put(key, value); err != nil {
		panic(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bitcask.Get(key); err != nil {
			panic(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	defer os.RemoveAll(dir)

	bitcask, err := Open(dir)
	if err != nil {
		panic(err)
	}

	key := []byte("key")
	value := []byte("value")

	if err := bitcask.Put(key, value); err != nil {
		panic(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bitcask.Get(key); err != nil {
				panic(err)
			}
		}
	})
}
