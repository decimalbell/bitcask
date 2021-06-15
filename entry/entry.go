package entry

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

type Entry struct {
	CRC       uint32
	Timestamp uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

func (e *Entry) Size() int {
	return 16 + len(e.Key) + len(e.Value)
}

func (e *Entry) IsDeleted() bool {
	return e.ValueSize == 0
}

func EncodedLen(key, value []byte) int {
	return 16 + len(key) + len(value)
}

func Encode(key, value []byte, ts uint32) []byte {
	size := 16 + len(key) + len(value)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[4:], uint32(ts))
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(value)))
	copy(buf[16:], key)
	copy(buf[16+len(key):], value)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf, crc)
	return buf
}

type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: bufio.NewReader(r),
	}
}

func NewReaderSize(r io.Reader, size int) *Reader {
	return &Reader{
		r: bufio.NewReaderSize(r, size),
	}
}

func (r *Reader) Read() (*Entry, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(buf[0:4])
	timestamp := binary.LittleEndian.Uint32(buf[4:8])
	keySize := binary.LittleEndian.Uint32(buf[8:12])
	valueSize := binary.LittleEndian.Uint32(buf[12:16])

	key := make([]byte, keySize)
	if _, err := io.ReadFull(r.r, key); err != nil {
		return nil, err
	}

	value := make([]byte, valueSize)
	if _, err := io.ReadFull(r.r, value); err != nil {
		return nil, err
	}

	return &Entry{
		CRC:       crc,
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}, nil
}
