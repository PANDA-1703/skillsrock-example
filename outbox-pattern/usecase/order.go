package usecase

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"outbox_pattern/entity"
	"outbox_pattern/repository"
	"time"
)

type OrderUsecase struct {
	db         *sql.DB
	orderRepo  *repository.OrderRepository
	outboxRepo *repository.OutboxRepository
}

func NewOrderUsecase(db *sql.DB, orderRepo *repository.OrderRepository, outboxRepo *repository.OutboxRepository) *OrderUsecase {
	return &OrderUsecase{
		db:         db,
		orderRepo:  orderRepo,
		outboxRepo: outboxRepo,
	}
}

// CreatedOrder - создать заказ
func (uc *OrderUsecase) CreateOrder(ctx context.Context, userID int, amount int64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}

    // Начало транзакции
	tx, err := uc.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

    // Создаем заказ
	order := &entity.Order{
		UserID: userID,
		Amount: amount,
		Status: "created",
	}

	if err := uc.orderRepo.CreateOrder(ctx, tx, order); err != nil {
		return fmt.Errorf("failed to create order: %v", err)
	}

    // Публикуем событие в брокер сообщений
	event := &entity.OrderEvent{
		OrderID: order.ID,
		UserID:  userID,
		Amount:  amount,
		Status:  "created",
	}

    // Сериализуем в JSON
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

    // Пишем JSON в Go-структуру
	outboxMsg := &entity.OutboxMessage{
		EventType: "order_created",
		Payload:   payload,
		CreatedAt: time.Now(),
		Sent:      false,
		Attempts:  0,
	}

	if err := uc.outboxRepo.AddToOutbox(ctx, tx, outboxMsg); err != nil {
		return fmt.Errorf("failed to add to outbox: %v", err)
	}

    // Фиксируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
