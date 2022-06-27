package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"queue/options"
)

type ActivationMessageHandler func(ctx context.Context, r MediaGridActivation) error

func (q *QueueHandler) AddActivationMessageHandler(handler ActivationMessageHandler) *QueueHandler {
	return q.AddMediaGridActivationHandler(func(ctx context.Context, r MediaGridActivation) error {
		fmt.Printf("%+v", r)
		err := handler(ctx, r)
		if err != nil {
			return fmt.Errorf("handling message: %w", err)
		}
		return nil
	})
}

type ActivationMessagePublisher interface {
	PublishActivationMessage(m MediaGridActivation, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishActivationMessage(m MediaGridActivation, opts ...options.PublishOptions) error {
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
