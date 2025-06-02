// Обработка outbox
package worker

import (
    "context"
    "encoding/json"
    "log"
    "outbox_pattern/entity"
    "outbox_pattern/repository"
    "time"
)

type OutboxWorker struct {
    outboxRepo *repository.OutboxRepository
    interval   time.Duration
}

func NewOutboxWorker(outboxRepo *repository.OutboxRepository, interval time.Duration) *OutboxWorker {
    return &OutboxWorker{
        outboxRepo: outboxRepo,
        interval:   interval,
    }
}

// Start - фоновый процесс. Каждые 5 сек достаёт неотправленные ивенты
func (w *OutboxWorker) Start(ctx context.Context) {
    ticker := time.NewTicker(w.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            w.processOutbox(ctx)
        case <-ctx.Done():
            log.Println("Outbox worker stopped.")
            return
        }
    }
}

// processOutbox - десереализация json в структуру, пометка отправленных ивентов
func (w *OutboxWorker) processOutbox(ctx context.Context) {
    messages, err := w.outboxRepo.GetUnsentMessages(ctx)
    if err != nil {
        log.Printf("Failed to get unsent messages: %v", err)
        return
    }

    for _, msg := range messages {
        var event entity.OrderEvent
        if err := json.Unmarshal(msg.Payload, &event); err != nil {
            log.Printf("Failed to unmarshal event: %v", err)
            continue
        }

		// TODO: Отправить ивент в брокер 

        log.Printf("Processed event: %+v", event)

        if err := w.outboxRepo.TagAsSent(ctx, msg.ID); err != nil {
            log.Printf("Failed to mark message as sent: %v", err)
        }
    }
}
