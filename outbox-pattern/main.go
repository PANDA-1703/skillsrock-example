package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"outbox_pattern/db"
	"outbox_pattern/repository"
	"outbox_pattern/usecase"
	"outbox_pattern/worker"
	"syscall"
	"time"
)

func main() {
	// Коннект в БД
	dsn := "postgres://user:password@localhost:5432/outbox_db?sslmode=disable"
	ctx := context.Background()

	pool, err := db.NewPgxPool(ctx, dsn)
	if err != nil {
		log.Fatalf("failed connect to db: %v", err)
	}
	defer pool.Close()

	txManager := repository.NewPgxManager(pool)

	orderRepo := repository.NewOrderRepository(pool)
	outboxRepo := repository.NewOutboxRepository(pool)
	orderUC := usecase.NewOrderUsecase(txManager, orderRepo, outboxRepo)

	// Создание заказа
	if err := orderUC.CreateOrder(ctx, 1, 100); err != nil {
		log.Printf("failed to create order: %v", err)
	}

	// Запуск воркера
	outboxWorker := worker.NewOutboxWorker(outboxRepo, 5*time.Second)
	go func() {
        if err := outboxWorker.Start(ctx); err != nil{
            log.Fatalf("failed running outboxworker: %v", err)
        }
    }()

    // graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <- quit
    log.Println("Finishing working...")

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    <-ctx.Done()
    log.Println("Graceful shutdown")
	
}
