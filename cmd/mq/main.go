package main

import (
	"github.com/mangoim/simple-mq/mq"
)

func main() {
	opt := &mq.Options{TcpAddress: "0.0.0.0:8000"}
	MQ := mq.New(opt)
	MQ.Run()
}
