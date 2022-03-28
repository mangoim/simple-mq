package mq

import (
	"bufio"
	"net"
	"sync"
)

const defaultBufferSize = 16 * 1024

type client struct {
	ID int64
	net.Conn

	writeLock sync.RWMutex

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer
}

func newClient(id int64, conn net.Conn) *client {
	return &client{
		ID:     id,
		Conn:   conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),
	}
}

func (c *client) String() string {
	return c.RemoteAddr().String()
}
