package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"queue/options"
)

type EmailMessageHandler func(ctx context.Context, r EmailMessage) error

func (q *QueueHandler) AddEmailMessageHandler(handler EmailMessageHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg EmailMessage
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

type EmailMessagePublisher interface {
	PublishEmailMessage(m EmailMessage, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishEmailMessage(m EmailMessage, opts ...options.PublishOptions) error {
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
