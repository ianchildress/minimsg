package protocol

import (
	"encoding/binary"
	"fmt"
)

// MarshalMessagePayload serializes a topic and message body into a single []byte.
// Format: [2B topicLen][topic][4B bodyLen][body]
func MarshalMessagePayload(topic string, body []byte) []byte {
	tlen := len(topic)
	blen := len(body)

	// allocate: 2 bytes for tlen, tlen bytes topic, 4 bytes for blen, blen bytes body
	buf := make([]byte, 2+tlen+4+blen)

	// write topic length
	binary.BigEndian.PutUint16(buf[0:2], uint16(tlen))
	// write topic bytes
	copy(buf[2:2+tlen], topic)
	// write body length
	binary.BigEndian.PutUint32(buf[2+tlen:2+tlen+4], uint32(blen))
	// write body bytes
	copy(buf[2+tlen+4:], body)

	return buf
}

// UnmarshalMessagePayload parses buf back into topic and body.
// Returns an error if buf is malformed or too short.
func UnmarshalMessagePayload(buf []byte) (topic string, body []byte, err error) {
	// need at least 2 bytes for topic length
	if len(buf) < 2 {
		return "", nil, fmt.Errorf("buffer too small for topic length")
	}
	tlen := int(binary.BigEndian.Uint16(buf[0:2]))
	// need 2 + tlen + 4 bytes at minimum
	if len(buf) < 2+tlen+4 {
		return "", nil, fmt.Errorf("buffer too small for topic+body length")
	}

	// extract topic
	topic = string(buf[2 : 2+tlen])

	// read body length
	offset := 2 + tlen
	blen := int(binary.BigEndian.Uint32(buf[offset : offset+4]))
	offset += 4

	// ensure buffer holds full body
	if len(buf) < offset+blen {
		return "", nil, fmt.Errorf("buffer too small for body (have %d, need %d)", len(buf)-offset, blen)
	}

	body = buf[offset : offset+blen]
	return topic, body, nil
}
