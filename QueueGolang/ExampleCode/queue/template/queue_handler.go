package template

import (
	"context"
	"encoding/json"
	"fmt"
	"queue/options"
)

type MessageType struct{}

type QueueHandler struct {
	AddHandler func(func(ctx context.Context, data []byte) error) *QueueHandler
	Publish    func(data []byte, opts ...options.PublishOptions) error
}

type MessageTypeHandler func(ctx context.Context, r MessageType) error

func (q *QueueHandler) AddMessageTypeHandler(handler MessageTypeHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg MessageType
		err := json.Unmarshal(data, &msg)
		if err != nil {
			return fmt.Errorf("parsing message json: %w", err)
		}
		err = handler(ctx, msg)
		if err != nil {
			return fmt.Errorf("handling message: %w", err)
		}
		return nil
	})
}

type MessageTypePublisher interface {
	PublishMessageType(m MessageType, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishMessageType(m MessageType, opts ...options.PublishOptions) error {
	byt, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling message into json: %w", err)
	}
	err = q.Publish(byt, opts...)
	if err != nil {
		return fmt.Errorf("publishing to queue: %w", err)
	}
	return nil
}
