package worker

import (
	"context"
	"outbox_pattern/entity"
)

type MockBroker struct{}

func NewBroker() MessageBroker {
	return &MockBroker{}
}

func (b *MockBroker) PublishOrderEvent(ctx context.Context, event *entity.OrderEvent) error {
	return nil
}
