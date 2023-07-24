package lib

import (
	"fmt"
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
	stage map[string]*chan bool
}

type ClientSQS struct {
	routerQueues map[string]queueRouter
	maxRetries   uint
}

func NewClient(limit int64, maxRetries uint, queues ...string) SQS {
	routerQueues := make(map[string]queueRouter, len(queues))
	for _, queueName := range queues {
		queue := make(chan Message, limit)
		dlq := make(chan Message, limit)

		routerQueues[queueName] = queueRouter{
			queue: &queue,
			dlq:   &dlq,
		}
	}
	return &ClientSQS{routerQueues: routerQueues}
}

func (c ClientSQS) SendMessage(queueName, data string) error {
	message := NewMessage(data)
	route, has := c.routerQueues[queueName]
	if !has {
		return fmt.Errorf("Queue %s not found", queueName)
	}
	*route.queue <- message
	return nil
}

func (c ClientSQS) ReceiveMessages(queue string, timeout, maxNum int64) (MessagesOutput, error) {
	var output MessagesOutput
	route, has := c.routerQueues[queue]
	if !has {
		return output, fmt.Errorf("Queue %s not found", queue)
	}

	for i := 1; i <= int(maxNum); i++ {
		msg := <-*route.queue
		msg.counterProcessed++
		output.Messages = append(output.Messages, msg)

		ack := make(chan bool)
		c.routerQueues[queue].stage[*msg.ID] = &ack
		go c.observer(queue, &msg, &ack, timeout)
	}

	return output, nil
}

func (c ClientSQS) observer(queue string, message *Message, ack *chan bool, timeout int64) {
	t := time.After(time.Duration(timeout) * time.Second)

	route := c.routerQueues[queue]

	select {
	case <-*ack:
		delete(c.routerQueues[queue].stage, *message.ID)
		close(*ack)
	case <-t:
		if message.counterProcessed == c.maxRetries {
			message.counterProcessed = 0
			*route.dlq <- *message
		} else {
			message.counterProcessed++
			*route.queue <- *message
		}
		delete(c.routerQueues[queue].stage, *message.ID)
		close(*ack)
	}
}

func (c ClientSQS) AckMessage(queue, messageID string) error {
	route, has := c.routerQueues[queue]
	if !has {
		return fmt.Errorf("Queue %s not found", queue)
	}

	ack, has := route.stage[messageID]
	if !has {
		return fmt.Errorf("Message %s not consumed", messageID)
	}
	*ack <- true
	return nil
}

func (c ClientSQS) CountMessagesQueue(queue string) (int, error) {
	if route, has := c.routerQueues[queue]; !has {
		return 0, fmt.Errorf("Queue %s not found", queue)
	} else {
		return len(*route.queue), nil
	}
}

func (c ClientSQS) CountMessagesDeadQueue(queue string) (int, error) {
	if route, has := c.routerQueues[queue]; !has {
		return 0, fmt.Errorf("Queue %s not found", queue)
	} else {
		return len(*route.dlq), nil
	}
}
