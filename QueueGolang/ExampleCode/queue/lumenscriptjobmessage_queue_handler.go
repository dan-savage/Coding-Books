package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"queue/options"
)

type LumenScriptJobMessageHandler func(ctx context.Context, r LumenScriptJobMessage) error

func (q *QueueHandler) AddLumenScriptJobMessageHandler(handler LumenScriptJobMessageHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg LumenScriptJobMessage
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

type LumenScriptJobMessagePublisher interface {
	PublishLumenScriptJobMessage(m LumenScriptJobMessage, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishLumenScriptJobMessage(m LumenScriptJobMessage, opts ...options.PublishOptions) error {
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
