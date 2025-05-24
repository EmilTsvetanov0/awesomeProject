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

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

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

	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>
}
