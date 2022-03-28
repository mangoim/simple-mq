package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
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
		command := []byte("PUB poetry\n")
		fourByte := make([]byte, 4)
		now := time.Now().String()
		body := []byte("~O Captain! my Captain! our fearful trip is done,\nThe ship has weather’d every rack, the prize we sought is won,\nThe port is near, the bells I hear, the people all exulting,\nWhile follow eyes the steady keel, the vessel grim and daring......~" + now)
		bodyLen := len(body)

		binary.BigEndian.PutUint32(fourByte, uint32(bodyLen))

		buf.Write(command)
		buf.Write(fourByte)
		buf.Write(body)

		_, err = conn.Write(buf.Bytes())
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Wait for reply")

		buffer := make([]byte, 2048)
		mLen, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		fmt.Println("Received: ", string(buffer[:mLen]))
		time.Sleep(3 * time.Second)
	}

}
