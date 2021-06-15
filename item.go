package bitcask

type item struct {
	fileID      uint32
	valueSize   uint32
	valueOffset uint32
	timestamp   uint32
}
