package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ianchildress/minimsg/broker"
)

var (
	addr = flag.String("addr", ":8080", "listen address")
)

func main() {
	flag.Parse()

	// Build plain TCP listener
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}

	// Context that cancels on SIGINT/SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Inject listener into Broker and serve
	b := broker.NewBroker(ln)
	if err := b.Serve(ctx); err != nil {
		log.Fatalf("broker Serve error: %v", err)
	}

	log.Println("broker shut down cleanly")
}
