package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tidwall/redcon"
)

var addr = ":6380"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("missing database")
	}
	log.Printf("started server at %s", addr)

	handler, err := NewHandler(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		handler.Close()
		log.Printf("stopped server")
		os.Exit(0)
	}()

	mux := redcon.NewServeMux()
	mux.HandleFunc("detach", handler.detach)
	mux.HandleFunc("ping", handler.ping)
	mux.HandleFunc("quit", handler.quit)
	mux.HandleFunc("set", handler.set)
	mux.HandleFunc("setnx", handler.setnx)
	mux.HandleFunc("get", handler.get)
	mux.HandleFunc("del", handler.delete)
	mux.HandleFunc("info", handler.info)
	mux.HandleFunc("scan", handler.scan)

	err = redcon.ListenAndServe(addr,
		mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
