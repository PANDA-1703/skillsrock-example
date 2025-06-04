package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"outbox_pattern/entity"
	"time"

	"github.com/jackc/pgx/v5"
)

// Tx - общий интерфейс для транзакций
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// TxManager - начала транзакций
type TxManager interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)
}


// OrderRepository - создание заказа
type OrderRepository interface{
	CreateOrder(ctx context.Context, tx pgx.Tx, order *entity.Order) error
}

// OutboxRepository - добавление в outbox
type OutboxRepository interface {
	AddToOutbox(ctx context.Context, tx pgx.Tx, message *entity.OutboxMessage) error
}

// OrderUsecase - мейн юзкейс
type OrderUsecase struct {
	txManager TxManager
	orderRepo  OrderRepository
	outboxRepo OutboxRepository
}

// NewOrderUsecase - конструктор
func NewOrderUsecase(
	txManager TxManager,
	orderRepo OrderRepository,
	outboxRepo OutboxRepository,
) *OrderUsecase {
	return &OrderUsecase{
		txManager: txManager,
		orderRepo: orderRepo,
		outboxRepo: outboxRepo,
	}
}

// CreatedOrder - создать заказ
func (uc *OrderUsecase) CreateOrder(ctx context.Context, userID int, amount int64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}

    // Начало транзакции
	tx, err := uc.txManager.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback(ctx)

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
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
