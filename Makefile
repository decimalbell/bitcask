BITCASK_SERVER_PKG=github.com/decimalbell/bitcask/cmd/server

build:
	go build -o bin/bitcask-server ${BITCASK_SERVER_PKG}
run:
	mkdir -p bin && cd bin && go run ${BITCASK_SERVER_PKG}
test:
	go test -race
bench:
	go test -bench=. -benchmem -benchtime=3s
