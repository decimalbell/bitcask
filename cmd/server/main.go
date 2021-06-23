package main

import (
	"flag"
	"log"
)

var (
	dir  = flag.String("dir", "../data", "dir")
	addr = flag.String("addr", "0.0.0.0:9736", "addr")
)

func main() {
	flag.Parse()
	s, err := newServer(*dir, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(s.listenAndServe())
}
