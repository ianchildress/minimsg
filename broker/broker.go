package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ianchildress/enproto"
	"github.com/ianchildress/minimsg/protocol"
)

/*
[offset][timestamp][payload]
*/

// Message represents one record in the log.
type Message struct {
	Offset    int64  // global offset within the topic
	Timestamp int64  // when it was appended
	Payload   []byte // raw user data
}

// Segment holds a contiguous run of Messages.
type Segment struct {
	BaseOffset int64     // Offset of the first message in this segment
	Messages   []Message // in-memory buffer (active segment)
	filePath   string    // on-disk data file for sealed segments
	indexPath  string    // on-disk index file mapping offsets → byte positions
	mu         sync.RWMutex
}

// Topic manages a sequence of segments and hands out offsets.
type Topic struct {
	mu         sync.RWMutex
	segments   []*Segment // sealed + active segments in ascending BaseOffset order
	nextOffset int64      // the next Offset to assign when appending
	// you could also track:
	//   retentionPolicy, maxSegmentBytes, maxSegmentAge, etc.
}

// Append adds a new message to the active segment (sealing & rolling if needed).
func (t *Topic) Append(payload []byte) (Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg := Message{
		Offset:    t.nextOffset,
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	seg := t.segments[len(t.segments)-1]
	seg.mu.Lock()
	seg.Messages = append(seg.Messages, msg)
	seg.mu.Unlock()

	t.nextOffset++
	// if seg exceeds size or age thresholds: sealAndRoll()
	return msg, nil
}

// Fetch returns up to max messages starting at fromOffset.
func (t *Topic) Fetch(fromOffset int64, max int) ([]Message, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// locate the segment containing fromOffset,
	// then read from either memory or disk as appropriate.
	// return up to 'max' messages.
	return nil, nil
}

// Broker holds all topics and orchestrates listening.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	ln     net.Listener
}

// NewBroker constructs a Broker with the given listener.
func NewBroker(ln net.Listener) *Broker {
	return &Broker{
		ln:     ln,
		topics: make(map[string]*Topic),
	}
}

// Serve starts accepting connections until ctx is canceled, then cleans up.
func (b *Broker) Serve(ctx context.Context) error {
	defer b.ln.Close()
	log.Printf("broker serving on %s", b.ln.Addr())

	// Close listener when context is done to unblock Accept().
	go func() { <-ctx.Done(); b.ln.Close() }()

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

// handleConn reads frames and dispatches based on message type.
func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	fr := enproto.NewFramer(conn)

	for {
		bType, payload, err := fr.ReadFrame()
		if err != nil {
			// Graceful client disconnect
			if errors.Is(err, io.EOF) {
				log.Printf("client %s closed connection", conn.RemoteAddr())
				return
			}
			log.Printf("read frame error from %s: %v", conn.RemoteAddr(), err)
			return
		}

		mt := protocol.MsgType(bType)
		switch mt {
		case protocol.MsgTypeEcho:
			b.handleEcho(fr, payload)
		case protocol.MsgTypeMetrics:
			b.handleMetrics(fr, payload)
		case protocol.MsgTypeCreateTopic:
			b.handleCreateTopic(fr, payload)
		case protocol.MsgTypeDeleteTopic:
			b.handleDeleteTopic(fr, payload)
		case protocol.MsgTypeDescribeTopic:
			b.handleDescribeTopic(fr, payload)
		case protocol.MsgTypePublish:
			b.handlePublish(fr, payload)
		case protocol.MsgTypeCommitOffset:
			b.handleCommitOffset(fr, payload)
		case protocol.MsgTypeHeartbeat:
			b.handleHeartbeat(fr)
		case protocol.MsgTypeListTopics:
			b.handleListTopics(fr)
		default:
			errMsg := fmt.Sprintf("unknown message type %d", bType)
			fr.WriteFrame(byte(protocol.MsgTypeError), []byte(errMsg))
		}
	}
}

// Stub handlers below. Fill in real logic as you expand.
func (b *Broker) handleEcho(fr *enproto.Framer, payload []byte) {
	// Echo back
	fr.WriteFrame(protocol.MsgTypeEcho.Byte(), payload)
}

func (b *Broker) handleMetrics(fr *enproto.Framer, payload []byte) {
	log.Printf("metrics: %s", string(payload))
}

//func (b *Broker) handleCreateTopic(fr *enproto.Framer, payload []byte) {
//	topic := string(payload)
//	b.mu.Lock()
//
//	// TODO FILL THIS IN
//
//	b.mu.Unlock()
//	fr.WriteFrame(byte(protocol.MsgTypeCreateTopic), []byte("ok"))
//}

func (b *Broker) handleDeleteTopic(fr *enproto.Framer, payload []byte) {
	topic := string(payload)
	b.mu.Lock()
	delete(b.topics, topic)
	b.mu.Unlock()
	fr.WriteFrame(byte(protocol.MsgTypeDeleteTopic), []byte("ok"))
}

func (b *Broker) handleDescribeTopic(fr *enproto.Framer, payload []byte) {
	topic := string(payload)
	b.mu.RLock()
	_, exists := b.topics[topic]
	b.mu.RUnlock()
	resp := []byte(fmt.Sprintf("exists=%v", exists))
	fr.WriteFrame(byte(protocol.MsgTypeDescribeTopic), resp)
}

//func (b *Broker) handlePublish(fr *enproto.Framer, payload []byte) {
//	topic, msg, err := protocol.UnmarshalMessagePayload(payload)
//	b.mu.RUnlock()
//}

func (b *Broker) handleCommitOffset(fr *enproto.Framer, payload []byte) {
	// Not implemented: store offsets per consumer group
	fr.WriteFrame(byte(protocol.MsgTypeCommitOffset), []byte("ok"))
}

func (b *Broker) handleHeartbeat(fr *enproto.Framer) {
	fr.WriteFrame(byte(protocol.MsgTypeHeartbeat), nil)
}

func (b *Broker) handleListTopics(fr *enproto.Framer) {
	b.mu.RLock()
	names := make([]string, 0, len(b.topics))
	for t := range b.topics {
		names = append(names, t)
	}
	b.mu.RUnlock()
	topicsPayload := []byte(strings.Join(names, ","))
	fr.WriteFrame(byte(protocol.MsgTypeListTopicsResponse), topicsPayload)
}

// =============== File Operations ===============

const indexEntrySize = 16 // Offset(8) + FilePosition(8)

// findIndexEntry opens the index file at idxPath and binary-searches for the exact entry
// whose Offset == targetOffset.  It returns the corresponding FilePosition, or an error
// if no such offset is found.
func findIndexEntry(idxPath string, targetOffset int64) (filePos int64, err error) {
	f, err := os.Open(idxPath)
	if err != nil {
		return 0, fmt.Errorf("open index file: %w", err)
	}
	defer f.Close()

	// figure out how many entries we have
	fi, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat index file: %w", err)
	}
	totalEntries := fi.Size() / indexEntrySize
	if totalEntries == 0 {
		return 0, fmt.Errorf("empty index")
	}

	low, high := int64(0), totalEntries-1
	buf := make([]byte, indexEntrySize)

	for low <= high {
		mid := (low + high) / 2
		off := mid * indexEntrySize

		// read one index entry
		if _, err := f.ReadAt(buf, off); err != nil {
			return 0, fmt.Errorf("read index at entry %d: %w", mid, err)
		}

		recOffset := int64(binary.BigEndian.Uint64(buf[0:8]))
		recPos := int64(binary.BigEndian.Uint64(buf[8:16]))

		switch {
		case recOffset == targetOffset:
			return recPos, nil

		case recOffset < targetOffset:
			low = mid + 1

		default:
			high = mid - 1
		}
	}

	return 0, fmt.Errorf("offset %d not found in index", targetOffset)
}
