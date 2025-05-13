package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ianchildress/minimsg/protocol"
	"log"
	"net"
	"os"
	"strings"

	"github.com/ianchildress/enproto"
)

var addr = flag.String("addr", "127.0.0.1:8080", "broker address (host:port)")

func main() {
	flag.Parse()

	// Connect to the broker
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *addr, err)
	}
	defer conn.Close()

	fr := enproto.NewFramer(conn)
	stdin := bufio.NewReader(os.Stdin)

	fmt.Printf("Connected to broker at %s. Type messages and press Enter to send.\n", *addr)
	for {
		// Prompt
		fmt.Print("> ")
		// Read input line
		line, err := stdin.ReadString('\n')
		if err != nil {
			log.Printf("error reading input: %v", err)
			break
		}
		// Trim newline
		line = strings.TrimSuffix(line, "\n")
		if len(line) == 0 {
			continue
		}

		// Send the message with msgType=1
		if err := fr.WriteFrame(protocol.MsgTypeEcho.Byte(), []byte(line)); err != nil {
			log.Printf("error writing frame: %v", err)
			break
		}

		// Wait for and read the broker's response
		msgType, payload, err := fr.ReadFrame()
		if err != nil {
			log.Printf("error reading response: %v", err)
			break
		}

		// Print the response
		fmt.Printf("Response (type %d): %s\n", msgType, string(payload))
	}
}
