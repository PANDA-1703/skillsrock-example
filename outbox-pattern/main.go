package main

import (
    "context"
    "database/sql"
    "log"
    "outbox_pattern/repository"
    "outbox_pattern/usecase"
    "outbox_pattern/worker"
    "time"

    _ "github.com/lib/pq"
)

func main() {
	// коннект в БД
    dsn := "postgres://user:password@localhost:5432/outbox_db?sslmode=disable"
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        log.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
	
    orderRepo := repository.NewOrderRepository(db)
    outboxRepo := repository.NewOutboxRepository(db)
    orderUC := usecase.NewOrderUsecase(db, orderRepo, outboxRepo)

    ctx := context.Background()

    // Создание заказа
    if err := orderUC.CreateOrder(ctx, 1, 100); err != nil {
        log.Printf("failed to create order: %v", err)
    }

    // Запуск воркера
    outboxWorker := worker.NewOutboxWorker(outboxRepo, 5*time.Second)
    go outboxWorker.Start(ctx)

    // Ожидание завершения
    select {}
}
