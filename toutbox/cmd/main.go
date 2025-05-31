package main

import (
	"context"
	"log"
	"sync"
	"toutbox/internal/kafka"
	"toutbox/internal/postgresql"
	pclient "toutbox/internal/postgresql/client"
	"toutbox/internal/utils"
)

// Service should just read postgres table "outbox" and post messages from there to kafka

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go utils.WaitForShutdownSignal(cancel)

	// PostgreSQL initialization
	newClient, err := pclient.NewClient(ctx)

	if err != nil {
		log.Fatal(err)
		return
	}

	repo := postgresql.NewPgClient(newClient, log.Default())

	// Starting kafka producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartProducer(ctx)
		if err != nil {
			log.Printf("[outbox] Kafka producer exited with error: %s\n", err)
		} else {
			log.Println("[outbox] Kafka producer exited successfully")
		}
	}()

	// Starting outbox dispatcher
	service := kafka.NewRunnerService(repo)

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("[outbox] Starting outbox dispatcher")
		service.StartOutboxDispatcher(ctx)
		log.Println("[outbox] Outbox dispatcher exited")
	}()

	wg.Wait()
}
