// Создание заказа
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"outbox_pattern/entity"
)

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

// Передаём транзакцию для атомарного создания заказа и пишем в outbox
func (r *OrderRepository) CreateOrder(ctx context.Context, tx *sql.Tx, order *entity.Order) error {
	query := "INSERT INTO orders (user_id, amount, status) VALUES ($1, $2, $3) RETURNING id"
	err := tx.QueryRowContext(ctx, query, order.UserID, order.Amount, order.Status).Scan(&order.ID)
	if err != nil {
		return fmt.Errorf("failed create order: %v", err)
	}
	return nil
}
