package main

import (
	"context"
	"errors"
	"log"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/decimalbell/bitcask"
)

var (
	errInvalidArgsLen = errors.New("bitcask: invalid args len")
)

type handler func(ctx context.Context, conn redcon.Conn, cmd redcon.Command) error

type server struct {
	bitcask *bitcask.Bitcask
	addr    string

	handlers map[string]handler
}

func newServer(dir string, addr string) (*server, error) {
	bitcask, err := bitcask.Open(dir)
	if err != nil {
		return nil, err
	}
	s := &server{
		bitcask:  bitcask,
		addr:     addr,
		handlers: make(map[string]handler),
	}
	s.init()
	return s, nil
}

func (s *server) init() {
	s.handlers["ping"] = s.ping
	s.handlers["get"] = s.get
	s.handlers["set"] = s.set
}

func (s *server) listenAndServe() error {
	return redcon.ListenAndServe(s.addr, s.handler, s.accept, s.closed)
}

func (s *server) handler(conn redcon.Conn, cmd redcon.Command) {
	log.Printf("handler conn: %v, cmd: %v", conn, cmd)
	name := strings.ToLower(string(cmd.Args[0]))
	handler, ok := s.handlers[name]
	if !ok {
		conn.WriteError("ERR Unknown or disabled command '" + string(cmd.Args[0]) + "'")
		return
	}
	ctx := context.Background()
	err := handler(ctx, conn, cmd)
	switch err {
	case nil:
	case errInvalidArgsLen:
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	default:
		conn.WriteError("ERR " + err.Error())
	}
}

func (s *server) accept(conn redcon.Conn) bool {
	log.Printf("accept conn: %v", conn)
	return true
}

func (s *server) closed(conn redcon.Conn, err error) {
	log.Printf("closed conn: %v", conn)
}

func (s *server) ping(ctx context.Context, conn redcon.Conn, cmd redcon.Command) error {
	str := "PONG"
	if len(cmd.Args) > 2 {
		return errInvalidArgsLen
	} else if len(cmd.Args) == 2 {
		str = string(cmd.Args[1])
	}
	conn.WriteString(str)
	return nil
}

func (s *server) get(ctx context.Context, conn redcon.Conn, cmd redcon.Command) error {
	if len(cmd.Args) != 2 {
		return errInvalidArgsLen
	}
	key := cmd.Args[1]
	value, err := s.bitcask.Get(ctx, key)
	if err != nil {
		return err
	}
	if value != nil {
		conn.WriteBulk(value)
	} else {
		conn.WriteNull()
	}
	return nil
}

func (s *server) set(ctx context.Context, conn redcon.Conn, cmd redcon.Command) error {
	if len(cmd.Args) != 3 {
		return errInvalidArgsLen
	}
	key := cmd.Args[1]
	value := cmd.Args[2]
	if err := s.bitcask.Put(ctx, key, value); err != nil {
		return err
	}
	conn.WriteString("OK")
	return nil
}
