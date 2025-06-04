// Обработка outbox
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"outbox_pattern/entity"
	"outbox_pattern/repository"
	"time"
)

type OutboxWorker struct {
    outboxRepo *repository.OutboxRepository
    broker MessageBroker
    interval   time.Duration
}

type OutboxRepository interface {
    GetUnsetMessages(ctx context.Context) ([]*entity.OutboxMessage, error)
    TagAsSent(ctx context.Context, id int64) error
}

type MessageBroker interface {
    PublishOrderEvent(ctx context.Context, event *entity.OrderEvent) error
}

func NewOutboxWorker(outboxRepo *repository.OutboxRepository, interval time.Duration) *OutboxWorker {
    return &OutboxWorker{
        outboxRepo: outboxRepo,
        interval:   interval,
    }
}

// Start - фоновый процесс. Каждые 5 сек достаёт неотправленные ивенты
func (w *OutboxWorker) Start(ctx context.Context) error {
    ticker := time.NewTicker(w.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            if err := w.processOutbox(ctx); err != nil {
                return fmt.Errorf("error: %v", err)
            }

        case <-ctx.Done():
            log.Println("Outbox worker stopped.")
            return fmt.Errorf("outbox stoppin work: %v", ctx.Err())
        }
    }
}

// processOutbox - десереализация json в структуру, отправка в брокер, пометка отправленных ивентов
func (w *OutboxWorker) processOutbox(ctx context.Context) error {
    messages, err := w.outboxRepo.GetUnsetMessages(ctx)
    if err != nil {
        return fmt.Errorf("failed to get unsent messages: %v", err)
    }

    for _, msg := range messages {
        var event entity.OrderEvent
        if err := json.Unmarshal(msg.Payload, &event); err != nil {
            log.Printf("Failed to unmarshal event: %v", err)
            continue
        }

		// Отправка в брокер
        if err := w.broker.PublishOrderEvent(ctx, &event); err != nil {
            log.Printf("Failed publish event in broker: %v", err)
            continue
        }

        log.Printf("Processed event: %+v", event)

        if err := w.outboxRepo.TagAsSent(ctx, msg.ID); err != nil {
            return fmt.Errorf("failed to mark message as sent: %v", err)
        } else {
            log.Printf("Published event in broker: %v", event)
        }
    }

    return nil
}
