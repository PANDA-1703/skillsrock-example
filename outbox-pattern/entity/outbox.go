package entity

import "time"

type OutboxMessage struct {
	ID        	int64
	EventType 	string			// тип события, например "order_created"
	Payload   	[]byte			// JSON данные
	CreatedAt 	time.Time
	Sent 		bool			// метка, было отправлено или нет
	SentAt 		*time.Time		// время отправки
	Attempts 	int				// сколько раз пытались отправить
}
