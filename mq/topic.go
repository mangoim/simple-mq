package mq

import (
	"log"
	"sync"
	"time"
)

type Topic struct {
	mq *MQ
	sync.RWMutex
	name          string
	memoryMsgChan chan *Message
	clients       map[int64]*client
	idFactory     *guidFactory
}

func NewTopic(topicName string, mq *MQ) *Topic {
	return &Topic{
		name:          topicName,
		mq:            mq,
		memoryMsgChan: make(chan *Message, 100),
		clients:       make(map[int64]*client),
		idFactory:     NewGUIDFactory(100),
	}
}

func (t *Topic) PutMessage(m *Message) {
	select {
	case t.memoryMsgChan <- m:
	default:
		log.Println("PutMessage: no message")
	}
}

func (t *Topic) PutClient(clientId int64, client *client) error {
	t.RLock()
	_, ok := t.clients[clientId]
	t.RUnlock()
	if ok {
		return nil
	}

	t.Lock()
	t.clients[clientId] = client
	t.Unlock()
	return nil
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			log.Printf("TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
