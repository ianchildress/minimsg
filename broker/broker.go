package broker

import (
	"context"
	"errors"
	"io"
	"log"
	"net"

	"github.com/ianchildress/enproto"
)

// Broker holds its listener and serves incoming connections.
type Broker struct {
	ln net.Listener
}

// NewBroker constructs a Broker with the given net.Listener.
func NewBroker(ln net.Listener) *Broker {
	return &Broker{ln: ln}
}

// Serve accepts connections until ctx is canceled, then cleans up.
func (b *Broker) Serve(ctx context.Context) error {
	defer b.ln.Close()
	log.Printf("broker serving on %s", b.ln.Addr())

	// Close listener when context is done to unblock Accept().
	go func() {
		<-ctx.Done()
		b.ln.Close()
	}()

	for {
		conn, err := b.ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil // clean shutdown
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		log.Printf("new client: %s", conn.RemoteAddr())
		go b.handleConn(conn)
	}
}

func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	fr := enproto.NewFramer(conn)

	for {
		msgType, payload, err := fr.ReadFrame()
		if err != nil {
			// Graceful shutdown on client close
			if errors.Is(err, io.EOF) {
				log.Printf("client %s closed connection", conn.RemoteAddr())
				return
			}
			var netErr *net.OpError
			if errors.As(err, &netErr) && netErr.Err.Error() == "use of closed network connection" {
				log.Printf("connection closed by broker for %s", conn.RemoteAddr())
				return
			}
			// Anything else is unexpected
			log.Printf("read frame error from %s: %v", conn.RemoteAddr(), err)
			return
		}

		log.Printf("received msgType=%d payload=%s", msgType, string(payload))

		if err := fr.WriteFrame(msgType, payload); err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("client %s closed connection during write", conn.RemoteAddr())
				return
			}
			log.Printf("write frame error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}
