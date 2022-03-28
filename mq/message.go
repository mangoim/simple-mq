package mq

import (
	"bytes"
	"encoding/binary"
	"time"
)

const (
	MsgIDLength = 16
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) Bytes() []byte {
	var binBuf [10]byte
	buf := new(bytes.Buffer)

	binary.BigEndian.PutUint64(binBuf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(binBuf[8:10], m.Attempts)

	buf.Write(binBuf[:])
	buf.Write(m.ID[:])
	buf.Write(m.Body)
	return buf.Bytes()
}
