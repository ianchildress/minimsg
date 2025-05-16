package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ianchildress/enproto"
	"github.com/ianchildress/minimsg/broker"
	"github.com/ianchildress/minimsg/protocol"
)

func TestBrokerEchoIntegration(t *testing.T) {
	// 1) pick a free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()

	// 2) start broker
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		b := broker.NewBroker(l)
		if err := b.Serve(ctx); err != nil {
			t.Errorf("broker exited: %v", err)
		}
	}()
	// give it a moment
	time.Sleep(50 * time.Millisecond)

	// 3) dial in as producer
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	fr := enproto.NewFramer(conn)

	// 4) send a message
	payload := []byte("hello integration")
	if err := fr.WriteFrame(protocol.MsgTypeEcho.Byte(), payload); err != nil {
		t.Fatalf("write frame: %v", err)
	}

	// 5) read reply
	typ, resp, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != protocol.MsgTypeEcho.Byte() {
		t.Errorf("expected echo type %d, got %d", protocol.MsgTypeEcho.Byte(), typ)
	}
	if string(resp) != string(payload) {
		t.Errorf("expected %q, got %q", payload, resp)
	}

	// 6) tear down
	cancel()
	conn.Close()
}

func TestBrokerCleanShutdownIntegration(t *testing.T) {
	// 1) pick a free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()

	// 2) start broker
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		b := broker.NewBroker(l)
		if err := b.Serve(ctx); err != nil {
			t.Errorf("broker exited with error: %v", err)
		}
	}()
	// give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// 3) request shutdown
	cancel()
	// allow Serve to close listener
	time.Sleep(50 * time.Millisecond)

	// 4) verify listener is closed
	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Error("expected connection failure after shutdown, but dial succeeded")
	}
}
