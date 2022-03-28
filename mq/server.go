package mq

import (
	"log"
	"net"
	"sync"
)

type Server struct {
	mq    *MQ
	conns sync.Map
}

func (s *Server) Run(tcpAddress string) {
	listener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		prot := &protocol{mq: s.mq}
		client := prot.NewClient(conn)
		s.conns.Store(conn.RemoteAddr(), client)
		go prot.IOLoop(client)
	}
}
