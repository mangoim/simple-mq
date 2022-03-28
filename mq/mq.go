package mq

import (
	"log"
	"sync"
)

type MQ struct {
	sync.RWMutex
	clientIDSequence int64
	server           *Server
	options          *Options
	topicMap         map[string]*Topic
}

func New(options *Options) *MQ {
	return &MQ{
		options:  options,
		topicMap: make(map[string]*Topic),
	}
}

func (m *MQ) Run() {
	server := Server{mq: m}
	server.Run(m.options.TcpAddress)
}

func (m *MQ) GetTopic(topicName string) *Topic {
	m.RLock()
	t, ok := m.topicMap[topicName]
	m.RUnlock()
	if ok {
		return t
	}

	m.Lock()
	t = NewTopic(topicName, m)
	m.topicMap[topicName] = t
	m.Unlock()

	log.Printf("TOPIC(%s): created", topicName)
	return t
}
