package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"queue/options"
)

type DV360ImportMessageHandler func(ctx context.Context, r DV360ImportMessage) error

func (q *QueueHandler) AddDV360ImportMessageHandler(handler DV360ImportMessageHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg DV360ImportMessage
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

type DV360ImportMessagePublisher interface {
	PublishDV360ImportMessage(m DV360ImportMessage, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishDV360ImportMessage(m DV360ImportMessage, opts ...options.PublishOptions) error {
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
