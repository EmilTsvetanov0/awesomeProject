package main

import (
	"context"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"producer/internal/kafka"
	"producer/internal/runner"
	"sync"
	"syscall"
)

func main() {

	brokers := viper.GetStringSlice("kafka.brokers")

	if err := kafka.InitKafka(brokers); err != nil {
		log.Fatal("Failed to init Kafka:", err)
	}
	defer func() {
		if err := kafka.CloseKafka(); err != nil {
			log.Fatal("Failed to close Kafka producer:", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	pool := runner.NewPool()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartConsumerGroup(ctx, "runners", "runner_group", pool)
		if err != nil {
			log.Println("Consumer group finished with error:", err)
			return
		}
	}()

	wg.Wait()
}
