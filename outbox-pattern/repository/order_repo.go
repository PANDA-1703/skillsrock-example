// Создание заказа
package repository

import (
	"context"
	"fmt"
	"outbox_pattern/entity"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

// CreateOrder - передаём транзакцию для атомарного создания заказа
func (r *OrderRepository) CreateOrder(ctx context.Context, tx pgx.Tx, order *entity.Order) error {
	query := "INSERT INTO orders (user_id, amount, status) VALUES ($1, $2, $3) RETURNING id"
	err := tx.QueryRow(ctx, query, order.UserID, order.Amount, order.Status).Scan(&order.ID)
	if err != nil {
		return fmt.Errorf("failed create order: %v", err)
	}
	return nil
}
