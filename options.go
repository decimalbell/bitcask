package bitcask

const (
	defaultMaxFileSize = 1e9
	defaultSyncOnPut   = false
)

var (
	defaultOptions = Options{
		maxFileSize: defaultMaxFileSize,
		syncOnPut:   defaultSyncOnPut,
	}
)

type Option func(*Options)

type Options struct {
	maxFileSize uint32
	syncOnPut   bool
}

func WithMaxFileSize(maxFileSize uint32) Option {
	return func(opts *Options) {
		opts.maxFileSize = maxFileSize
	}
}

func WithSyncOnPut(syncOnPut bool) Option {
	return func(opts *Options) {
		opts.syncOnPut = syncOnPut
	}
}
