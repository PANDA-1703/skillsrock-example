// Работа с БД
package repository

import (
	"context"
	"database/sql"
	"outbox_pattern/entity"
	"time"
)

type OutboxRepository struct {
	db *sql.DB
}

// NewOutboxRepository - конструктор - сохраняем ссылку на БД, чтоб использовать в методах
func NewOutboxRepository(db *sql.DB) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// AddToOutbox - добавить ивент в outbox
func (r *OutboxRepository) AddToOutbox(ctx context.Context, tx *sql.Tx, message *entity.OutboxMessage) error {
	query := "INSERT INTO outbox_messages (event_type, payload, created_at, sent, attempts) VALUES ($1, $2, $3, $4, $5)"
	_, err := tx.ExecContext(ctx, query, message.EventType, message.Payload, message.CreatedAt, message.Sent, message.Attempts)
	return err
}

// GetUnsetMessages - достать все неотправленные ивенты
func (r *OutboxRepository) GetUnsentMessages(ctx context.Context) ([]*entity.OutboxMessage, error) {
	query := "SELECT id, event_type, payload, created_at, sent, sent_at, attempts FROM outbox_messages WHERE sent = false"
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
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
	query := "UPDATE outbox_messages SET sent = true, sent_at = $1 WHERE id = $2"
	_, err := r.db.ExecContext(ctx, query, time.Now(), id)
	return err
}
