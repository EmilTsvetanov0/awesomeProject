package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"userapi/internal/kafka"
	"userapi/internal/postgresql"
	pclient "userapi/internal/postgresql/client"
	"userapi/internal/server"
)

func main() {

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	// PostgreSQL initialization
	newClient, err := pclient.NewClient(context.Background())

	if err != nil {
		log.Fatal(err)
		return
	}

	repository := postgresql.NewPgClient(newClient, log.Default())

	// Kafka consumer for transactional outbox
	wg.Add(1)

	rs := kafka.NewRunnerService(repository)

	topic := "outbox"

	go func() {
		defer wg.Done()
		err := rs.StartConsumer(ctx, topic)
		if err != nil {
			log.Printf("[userapi] Kafka consumer exited with error: %s\n", err)
		} else {
			log.Println("[userapi] Kafka consumer exited successfully")
		}
	}()

	// Kafka producer

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartProducer(ctx)
		if err != nil {
			log.Printf("[userapi] Kafka producer exited with error: %s\n", err)
		} else {
			log.Println("[userapi] Kafka producer exited successfully")
		}
	}()

	// Server init
	port := os.Getenv("PORT")
	log.Printf("listening on port %s", port)
	client := server.New(port)

	if err := client.Run(); err != nil {
		return
	}

	wg.Wait()
}
