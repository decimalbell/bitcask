package bitcask

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/decimalbell/bitcask/entry"
)

const (
	dataFilenamePrefix = "bitcask.data."
	hintFilenamePrefix = "bitcask.hint."
)

type Bitcask struct {
	dir     string
	options *Options

	keydir *keydir
	rfiles *sync.Map

	mu     sync.Mutex
	fileID uint32
	file   *os.File
	offset uint32
}

func Open(dir string, opts ...Option) (*Bitcask, error) {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}

	return open(dir, &options)
}

func open(dir string, options *Options) (*Bitcask, error) {
	if err := os.MkdirAll(dir, 0744); err != nil {
		return nil, err
	}
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	names, err := file.Readdirnames(0)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	rfiles := new(sync.Map)
	keydir := NewKeydir()
	for _, name := range names {
		fileID, err := dataFileID(name)
		if err != nil {
			return nil, err
		}
		file, err = loadDataFile(dir, name, fileID, keydir)
		if err != nil {
			return nil, err
		}
		rfiles.Store(fileID, file)
	}

	var (
		path   string
		fileID uint32
		offset uint32
	)
	if len(names) == 0 {
		fileID = 1
		path = dataFilepath(dir, fileID)
	} else {
		name := names[len(names)-1]
		fileID, _ = dataFileID(name)
		path = filepath.Join(dir, name)
	}

	flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
	if options.syncOnPut {
		flag |= os.O_SYNC
	}
	file, err = os.OpenFile(path, flag, 0644)
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	offset = uint32(fileInfo.Size())

	return &Bitcask{
		dir:     dir,
		options: options,
		keydir:  keydir,

		rfiles: rfiles,

		fileID: fileID,
		file:   file,
		offset: offset,
	}, nil
}

func dataFileID(name string) (uint32, error) {
	return fileID(name, dataFilenamePrefix)
}

func fileID(name, prefix string) (uint32, error) {
	n := strings.Index(name, prefix)
	if n != 0 {
		return 0, fmt.Errorf("bitcask: invalid name, name = %s, prefix = %s", name, prefix)
	}
	id, err := strconv.ParseUint(name[len(prefix):], 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

func dataFilepath(dir string, fileID uint32) string {
	filename := dataFilenamePrefix + strconv.FormatUint(uint64(fileID), 10)
	return filepath.Join(dir, filename)
}

func loadDataFile(dir string, name string, fileID uint32, keydir *keydir) (*os.File, error) {
	file, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	r := entry.NewReader(file)
	var offset uint32
	for {
		entry, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		offset += uint32(entry.Size())
		key := string(entry.Key)
		if entry.IsDeleted() {
			keydir.Delete(key)
			continue
		}
		item := &item{
			fileID:      fileID,
			valueSize:   entry.ValueSize,
			valueOffset: offset - entry.ValueSize,
			timestamp:   entry.Timestamp,
		}
		keydir.Put(key, item)
	}
	return file, nil
}

func (bitcask *Bitcask) Get(ctx context.Context, key []byte) ([]byte, error) {
	item, ok := bitcask.keydir.Get(string(key))
	if !ok {
		return nil, nil
	}

	var (
		rfile interface{}
		err   error
	)

	rfile, ok = bitcask.rfiles.Load(item.fileID)
	if !ok {
		var group singleflight.Group
		key := strconv.Itoa(int(item.fileID))
		rfile, err, _ = group.Do(key, func() (interface{}, error) {
			path := dataFilepath(bitcask.dir, item.fileID)
			file, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			bitcask.rfiles.Store(item.fileID, file)
			return file, nil
		})
		if err != nil {
			return nil, err
		}
	}

	file := rfile.(*os.File)
	value := make([]byte, int(item.valueSize))
	if _, err := file.ReadAt(value, int64(item.valueOffset)); err != nil {
		return nil, err
	}

	return value, nil
}

func (bitcask *Bitcask) Put(ctx context.Context, key, value []byte) error {
	ts := uint32(time.Now().Unix())
	buf := entry.Encode(key, value, ts)

	bitcask.mu.Lock()
	defer bitcask.mu.Unlock()

	if err := bitcask.putLocked(ctx, buf); err != nil {
		return err
	}
	item := &item{
		fileID:      bitcask.fileID,
		valueSize:   uint32(len(value)),
		valueOffset: bitcask.offset - uint32(len(value)),
		timestamp:   ts,
	}
	bitcask.keydir.Put(string(key), item)
	return nil
}

func (bitcask *Bitcask) putLocked(ctx context.Context, buf []byte) error {
	n := uint32(len(buf))
	if bitcask.offset+n > bitcask.options.maxFileSize {
		flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
		if bitcask.options.syncOnPut {
			flag |= os.O_SYNC
		}
		path := dataFilepath(bitcask.dir, bitcask.fileID+1)
		file, err := os.OpenFile(path, flag, 0644)
		if err != nil {
			return err
		}
		bitcask.file = file
		bitcask.fileID += 1
		bitcask.offset = 0
	}

	if _, err := bitcask.file.Write(buf); err != nil {
		return err
	}
	bitcask.offset += n
	return nil
}

func (bitcask *Bitcask) Delete(ctx context.Context, key []byte) error {
	ts := uint32(time.Now().Unix())
	buf := entry.Encode(key, []byte{}, ts)

	bitcask.mu.Lock()
	defer bitcask.mu.Unlock()

	if err := bitcask.putLocked(ctx, buf); err != nil {
		return err
	}
	bitcask.keydir.Delete(string(key))
	return nil
}

func (bitcask *Bitcask) Len() int {
	return bitcask.keydir.Len()
}

func (bitcask *Bitcask) Sync() error {
	bitcask.mu.Lock()
	defer bitcask.mu.Unlock()

	if bitcask.file == nil {
		return nil
	}
	return bitcask.file.Sync()
}

func (bitcask *Bitcask) Close() error {
	bitcask.mu.Lock()
	defer bitcask.mu.Unlock()

	if bitcask.file == nil {
		return nil
	}
	if err := bitcask.file.Sync(); err != nil {
		return err
	}
	return bitcask.file.Close()
}
