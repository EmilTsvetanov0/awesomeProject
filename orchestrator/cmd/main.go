package main

import (
	"context"
	"log"
	"orchestrator/internal/kafka"
	"orchestrator/internal/postgresql"
	pclient "orchestrator/internal/postgresql/client"
	"orchestrator/internal/runners"
	"orchestrator/internal/server"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	//Producer

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartProducer(ctx)
		if err != nil {
			log.Printf("[orchestrator] Kafka producer exited with error: %s\n", err)
		} else {
			log.Println("[orchestrator] Kafka producer exited successfully")
		}
	}()

	// TODO: Instead of this need to do transactional outbox
	// PostgreSQL initialization
	newClient, err := pclient.NewClient(context.Background())

	if err != nil {
		log.Fatal(err)
		return
	}

	repository := postgresql.NewPgClient(newClient, log.Default())

	// Runners pool

	runnerPool := runners.NewScenarioPool(
		func(ctx context.Context, id string, newStatus string) {
			if err := repository.UpdateScenarioStatus(ctx, id, newStatus); err != nil {
				log.Println("[orchestrator] UpdateScenarioStatus error: ", err)
			}
		},
	)

	// Consumer

	wg.Add(1)

	rs := kafka.NewRunnerService(runnerPool, repository)

	topic := "scenario"

	go func() {
		defer wg.Done()
		err := rs.StartConsumer(ctx, topic)
		if err != nil {
			log.Printf("[orchestrator] Kafka consumer exited with error: %s\n", err)
		} else {
			log.Println("[orchestrator] Kafka consumer exited successfully")
		}
	}()

	// Server init
	client := server.New("8080", repository, runnerPool)

	if err := client.Run(); err != nil {
		return
	}

	wg.Wait()

}
