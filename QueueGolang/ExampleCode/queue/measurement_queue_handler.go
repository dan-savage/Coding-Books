package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"code.avct.cloud/attention-measurement-platform/internal/queue/options"
)

type MeasurementHandler func(ctx context.Context, r Measurement) error

func (q *QueueHandler) AddMeasurementHandler(handler MeasurementHandler) *QueueHandler {
	return q.AddHandler(func(ctx context.Context, data []byte) error {
		var msg Measurement
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

type MeasurementPublisher interface {
	PublishMeasurement(m Measurement, opts ...options.PublishOptions) error
}

func (q *QueueHandler) PublishMeasurement(m Measurement, opts ...options.PublishOptions) error {
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
