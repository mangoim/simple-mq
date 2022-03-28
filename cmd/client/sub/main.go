package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

func main() {
	// connect to server
	conn, err := net.Dial("tcp", "127.0.0.1:8000")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("conn.LocalAddr %v", conn.LocalAddr())
	defer conn.Close()

	for {
		buf := new(bytes.Buffer)
		command := []byte("SUB poetry\n")
		buf.Write(command)
		_, err = conn.Write(buf.Bytes())
		if err != nil {
			log.Println("err", err)
			os.Exit(1)
		}

		log.Println("Wait for reply")
		buffer := make([]byte, 2048)
		_, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}

		// decode response
		size := binary.BigEndian.Uint32(buffer[0:])
		frameType := binary.BigEndian.Uint32(buffer[4:])
		log.Printf("size: %d, frameType: %d", size, frameType)

		// decode message
		if frameType == uint32(frameTypeMessage) {
			data := buffer[8:]
			timestamp := binary.BigEndian.Uint64(data[0:])
			attempts := binary.BigEndian.Uint16(data[8:])
			id := data[10:26]
			log.Printf("timestamp: %d, attempts: %d, id: %s", timestamp, attempts, id)
			log.Printf("Received: %s", string(data[26:]))
		}

		time.Sleep(1 * time.Second)
	}

}
