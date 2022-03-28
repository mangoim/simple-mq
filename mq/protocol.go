package mq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var okBytes = []byte("OK")

type protocol struct {
	mq *MQ
}

func (p *protocol) NewClient(conn net.Conn) *client {
	clientId := atomic.AddInt64(&p.mq.clientIDSequence, 1)
	return newClient(clientId, conn)
}

func (p *protocol) IOLoop(client *client) {
	var err error
	var line []byte

	// messagePump
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]

		// parse params
		params := bytes.Split(line, separatorBytes)

		response, err := p.Exec(client, params)
		if err != nil {
			log.Printf("response err %s", err)
			err = p.Send(client, frameTypeError, response)
			if err != nil {
				log.Printf("send err %s", err)
				break
			}
		}

		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

}

func (p *protocol) Exec(client *client, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	}
	return nil, errors.New("E_INVALID")
}

func (p *protocol) SUB(client *client, params [][]byte) ([]byte, error) {
	if len(params) < 2 {
		return nil, errors.New("E_INVALID")
	}
	topicName := string(params[1])
	t := p.mq.GetTopic(topicName)

	err := t.PutClient(client.ID, client)
	if err != nil {
		log.Printf("PutClient err %v", err)
	}
	if err != nil {
		return nil, errors.New("E_INVALID")
	}
	return okBytes, nil
}

func (p *protocol) PUB(client *client, params [][]byte) ([]byte, error) {
	if len(params) < 2 {
		return nil, errors.New("E_INVALID")
	}

	tmp := make([]byte, 4)
	bodyLen, err := readLen(client.Reader, tmp)
	if err != nil {
		return nil, errors.New("E_INVALID")
	}

	body := make([]byte, bodyLen)

	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, errors.New("E_INVALID")
	}
	topicName := string(params[1])
	t := p.mq.GetTopic(topicName)

	msg := NewMessage(t.GenerateID(), body)

	go t.PutMessage(msg)
	return okBytes, nil
}

func (p *protocol) messagePump(client *client, startedChan chan bool) {
	close(startedChan)

	// consumer msg
	for {
		for _, topic := range p.mq.topicMap {
			_, ok := topic.clients[client.ID]
			if ok {
				select {
				case msg := <-topic.memoryMsgChan:
					log.Printf("msgID %s", msg.ID)
					_ = p.SendMessage(client, msg)
				default:
					continue
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (p *protocol) SendMessage(client *client, msg *Message) error {
	err := p.Send(client, frameTypeMessage, msg.Bytes())
	if err != nil {
		log.Printf("Send err %s", err)
		return err
	}
	return err
}

func (p *protocol) Send(client *client, frameType int32, data []byte) error {
	client.writeLock.Lock()
	_, err := SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Writer.Flush()
	}

	client.writeLock.Unlock()
	return err
}

func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err

}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
