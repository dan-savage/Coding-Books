package options

import (
	"context"
	"testing"
)

func TestCorrelationID(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithPublishOptions(ctx, WithCorrelationID("test"))
	correlationID := CorrelationIDFromContext(ctx)
	if correlationID != "test" {
		t.Errorf("expected correlationID test, got %s", correlationID)
	}
}

func TestMerge(t *testing.T) {
	opts := Merge(WithCorrelationID("one"), WithCorrelationID("two"))
	ctx := context.Background()
	ctx = ContextWithPublishOptions(NewMessageContext(), opts)
	correlationID := CorrelationIDFromContext(ctx)
	if correlationID != "two" {
		t.Errorf("expected correlationID test, got %s", correlationID)
	}
}
