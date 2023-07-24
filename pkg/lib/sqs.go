package lib

import (
	"fmt"
	"sync"
	"time"
)

type SQS interface {
	SendMessage(queueName, data string) error
	ReceiveMessages(queue string, timeout, maxNum int64) (MessagesOutput, error)
	AckMessage(queue, messageID string) error
	CountMessagesQueue(queue string) (int, error)
	CountMessagesDeadQueue(queue string) (int, error)
}

type queueRouter struct {
	queue *chan Message
	dlq   *chan Message
	stage *sync.Map
}

type ClientSQS struct {
	routers    map[string]queueRouter
	maxRetries uint
}

func NewClient(limit int64, maxRetries uint, queues ...string) SQS {
	routers := make(map[string]queueRouter, len(queues))
	for _, queueName := range queues {
		queue := make(chan Message, limit)
		dlq := make(chan Message, limit)
		var stage sync.Map
		routers[queueName] = queueRouter{
			queue: &queue,
			dlq:   &dlq,
			stage: &stage,
		}
	}
	return &ClientSQS{routers: routers, maxRetries: maxRetries}
}

func (c ClientSQS) SendMessage(queueName, data string) error {
	message := NewMessage(data)
	route, has := c.routers[queueName]
	if !has {
		return fmt.Errorf("Queue %s not found", queueName)
	}
	*route.queue <- message
	return nil
}

func (c ClientSQS) ReceiveMessages(queue string, timeout, maxNum int64) (MessagesOutput, error) {
	var output MessagesOutput
	route, has := c.routers[queue]
	if !has {
		return output, fmt.Errorf("Queue %s not found", queue)
	}

	for i := 1; i <= int(maxNum) && len(*route.queue) > 0; i++ {
		msg := <-*route.queue
		msg.counterProcessed++
		output.Messages = append(output.Messages, msg)

		ack := make(chan bool)
		route.stage.Store(*msg.ID, &ack)
		go c.observer(queue, &msg, &ack, timeout)
	}

	return output, nil
}

func (c ClientSQS) observer(queue string, message *Message, ack *chan bool, timeout int64) {
	//t := time.After(time.Duration(timeout) * time.Second)

	route := c.routers[queue]

	select {
	case <-*ack:
		route.stage.Delete(*message.ID)
		close(*ack)
		return
	case <-time.After(time.Duration(timeout)*time.Second):
		if message.counterProcessed == c.maxRetries {
			message.counterProcessed = 0
			*route.dlq <- *message
		} else {
			message.counterProcessed++
			*route.queue <- *message
		}
		route.stage.Delete(*message.ID)
		close(*ack)
	}
}

func (c ClientSQS) AckMessage(queue, messageID string) error {
	route, has := c.routers[queue]
	if !has {
		return fmt.Errorf("Queue %s not found", queue)
	}

	ch, has := route.stage.Load(messageID)
	if !has {
		return fmt.Errorf("Message %s not consumed", messageID)
	}
	ack, _ := ch.(*chan bool)
	*ack <- true
	return nil
}

func (c ClientSQS) CountMessagesQueue(queue string) (int, error) {
	if route, has := c.routers[queue]; !has {
		return 0, fmt.Errorf("Queue %s not found", queue)
	} else {
		return len(*route.queue), nil
	}
}

func (c ClientSQS) CountMessagesDeadQueue(queue string) (int, error) {
	if route, has := c.routers[queue]; !has {
		return 0, fmt.Errorf("Queue %s not found", queue)
	} else {
		return len(*route.dlq), nil
	}
}
