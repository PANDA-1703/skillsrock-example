// Работа с БД
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"outbox_pattern/entity"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository - конструктор - сохраняем ссылку на БД, чтоб использовать в методах
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// AddToOutbox - добавить ивент в outbox
func (r *OutboxRepository) AddToOutbox(ctx context.Context, tx pgx.Tx, message *entity.OutboxMessage) error {
	query := "INSERT INTO outbox_messages (event_type, payload, created_at, sent, attempts) VALUES ($1, $2, $3, $4, $5)"
	_, err := tx.Exec(ctx, query, message.EventType, message.Payload, message.CreatedAt, message.Sent, message.Attempts)
	if err != nil {
		return fmt.Errorf("failed event add to outbox: %v", err)
	}
	return nil
}

// GetUnsetMessages - достать все неотправленные ивенты
func (r *OutboxRepository) GetUnsetMessages(ctx context.Context) ([]*entity.OutboxMessage, error) {
	query := "SELECT id, event_type, payload, created_at, sent, sent_at, attempts FROM outbox_messages WHERE sent = false"

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed get unset events: %v", err)
	}
	defer rows.Close()

	var messages []*entity.OutboxMessage
	for rows.Next() {
		var msg entity.OutboxMessage
		var sentAt sql.NullTime
		if err := rows.Scan(&msg.ID, &msg.EventType, &msg.Payload, &msg.CreatedAt, &msg.Sent, &sentAt, &msg.Attempts); err != nil {
			return nil, err
		}
		if sentAt.Valid {
			msg.SentAt = &sentAt.Time
		}
		messages = append(messages, &msg)
	}
	return messages, nil
}

// TagAsSent - пометить ивент как отправленный
func (r *OutboxRepository) TagAsSent(ctx context.Context, id int64) error {
	query := "UPDATE outbox_messages SET sent = true, sent_at = $1, attempts + 1 WHERE id = $2"
	_, err := r.pool.Exec(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed tag event as sent: %v", err)
	}
	return nil
}
