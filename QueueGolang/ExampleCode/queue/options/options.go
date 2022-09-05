package options

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
)

type PublishOptions struct {
	CorrelationID *string
}

func WithCorrelationID(correlationID string) PublishOptions {
	return PublishOptions{
		CorrelationID: &correlationID,
	}
}

func WithCorrelationIDFromContext(ctx context.Context) PublishOptions {
	opts, _ := PublishOptionsFromContext(ctx)
	return PublishOptions{
		CorrelationID: opts.CorrelationID,
	}
}

type publishOptionsKey struct{}

func ContextWithPublishOptions(parent context.Context, opts PublishOptions) context.Context {
	existing, ok := PublishOptionsFromContext(parent)
	if ok {
		opts = Merge(existing, opts)
	}
	return context.WithValue(parent, publishOptionsKey{}, opts)
}

func PublishOptionsFromContext(ctx context.Context) (PublishOptions, bool) {
	opts, ok := ctx.Value(publishOptionsKey{}).(PublishOptions)
	return opts, ok
}

func CorrelationIDFromContext(ctx context.Context) string {
	opts, _ := PublishOptionsFromContext(ctx)
	return aws.StringValue(opts.CorrelationID)
}

type deleteKey struct{}

type deleteValue struct {
	shouldDelete bool
}

// SetMessageDelete sets the message delete value on the message context. It will override any standard
// message delete behaviour.
func SetMessageDelete(parent context.Context, shouldDelete bool) {
	val, isSet := parent.Value(deleteKey{}).(*deleteValue)
	if isSet {
		val.shouldDelete = shouldDelete
	}
}

// GetMessageDeleteValue returns the message delete value and whether it has been set.
func GetMessageDeleteValue(ctx context.Context) (shouldDelete bool, isSet bool) {
	val, isSet := ctx.Value(deleteKey{}).(*deleteValue)
	if isSet {
		return val.shouldDelete, isSet
	}
	return false, false
}

// NewMessageContext creates a new message context assigning a correlation ID.
func NewMessageContext() context.Context {
	return ContextWithPublishOptions(context.Background(), PublishOptions{
		CorrelationID: aws.String(uuid.NewString()),
	})
}

func Merge(opts ...PublishOptions) PublishOptions {
	var p PublishOptions
	for _, opt := range opts {
		if opt.CorrelationID != nil {
			p.CorrelationID = opt.CorrelationID
		}
	}
	return p
}
