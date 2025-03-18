package main

import (
	"awesomeProject/internal/kafka"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
)

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {
	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>

	topic := "notifications"
	msgCnt := 0

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("ошибка чтения конфига: %w", err))
	}

	brokers := viper.GetStringSlice("brokers")
	worker, err := kafka.ConnectConsumer(brokers)
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Consumer started\n")

	// This shit for OS signals, very popular somehow
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create a gorouting to run the consumer / producer
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Printf("Consumer error: %s\n", err)
			case msg := <-consumer.Messages():
				msgCnt++
				fmt.Printf("Consumer message: %s with count: %d\n", string(msg.Value), msgCnt)
				notification := string(msg.Value)
				fmt.Printf("Notification: %s\n", notification)
			case <-sigchan:
				fmt.Printf("Shutting down\n")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

	fmt.Printf("Messages processed: %d\n", msgCnt)

	if err := consumer.Close(); err != nil {
		panic(err)
	}

}
