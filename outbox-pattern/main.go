package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
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
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbType := os.Getenv("DB")
	user := os.Getenv("DB_USER")
	passwd := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("%s://%s:%s@%s:%s/%s?sslmode=disable", dbType, user, passwd, host, port, dbName)

	ctx := context.Background()

	pool, err := db.NewPgxPool(ctx, dsn)
	if err != nil {
		log.Fatalf("failed connect to db: %v", err)
	}
	defer pool.Close()

	txManager := repository.NewPgx(pool)

	orderRepo := repository.NewOrderRepository(pool)
	outboxRepo := repository.NewOutboxRepository(pool)
	orderUC := usecase.New(txManager, orderRepo, outboxRepo)

	// Создание заказа
	if err := orderUC.CreateOrder(ctx, 1, 100); err != nil {
		log.Printf("failed to create order: %v", err)
	}

	// Брокер-заглушка
	mockBroker := worker.NewBroker()

	// Запуск воркера
	outboxWorker := worker.New(outboxRepo, mockBroker, 5*time.Second)
	go func() {
		if err := outboxWorker.Start(ctx); err != nil {
			log.Fatalf("failed running outboxworker: %v", err)
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Graceful shutdown..")

}
