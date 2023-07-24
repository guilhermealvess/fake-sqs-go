package lib

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID               *string
	Body             *string
	Timestamp        time.Time
	HashMD5          *string
	counterProcessed uint
}

func NewMessage(data string) Message {
	id := uuid.NewString()
	now := time.Now().UTC()

	src := fmt.Sprintf("id=%s;body=%s;timestamp=%v", id, data, now.String())
	hash := md5.Sum([]byte(src))
	hashString := hex.EncodeToString(hash[:])

	return Message{
		ID:               &id,
		Body:             &data,
		Timestamp:        now,
		HashMD5:          &hashString,
		counterProcessed: 0,
	}
}

type MessagesOutput struct {
	Messages []Message
}
