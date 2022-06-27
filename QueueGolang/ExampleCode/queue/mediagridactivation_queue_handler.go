package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"queue/options"
)

type MediaGridActivationHandler func(ctx context.Context, r MediaGridActivation) error

func (q *QueueHandler) AddMediaGridActivationHandler(handler MediaGridActivationHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg MediaGridActivation
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

type MediaGridActivationPublisher interface {
	PublishMediaGridActivation(m MediaGridActivation, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishMediaGridActivation(m MediaGridActivation, opts ...options.PublishOptions) error {
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
