package entity

import "time"

type OutboxMessage struct {
	ID        int64
	EventType string // Тип события, например "order_created"
	Payload   []byte // JSON данные
	CreatedAt time.Time
	Sent      bool       // Метка, было отправлено или нет
	SentAt    *time.Time // Время отправки
	Attempts  int        // Сколько раз пытались отправить
}
