package repository

import (
	"context"
	"fmt"
	"outbox_pattern/usecase"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgxManager struct {
	pool *pgxpool.Pool
}

func NewPgxManager(pool *pgxpool.Pool) usecase.TxManager {
	return &PgxManager{pool: pool}
}

type PgxTx struct {
	tx pgx.Tx
	conn *pgxpool.Conn
}

func (p *PgxManager) BeginTx(ctx context.Context) (pgx.Tx, error) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed asquire: %v", err)
	}
	
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("failed BeginTx: %v", err)
	}

	return tx, nil
}

func (t *PgxTx) Commit(ctx context.Context) error{
	err := t.tx.Commit(ctx)
	t.conn.Release()
	return fmt.Errorf("failed commit: %v", err)
}

func (t *PgxTx) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	t.conn.Release()
	return fmt.Errorf("failed rollback: %v", err)
}