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

//TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>

func main() {
	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>

	//topic := "notifications"
	//msgCnt := 0
	//
	//brokers := viper.GetStringSlice("brokers")
	//worker, err := kafka.ConnectConsumer(brokers)
	//if err != nil {
	//	panic(err)
	//}
	//
	//consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Printf("Consumer started\n")
	//
	//// This shit for OS signals, very popular somehow
	//sigchan := make(chan os.Signal, 1)
	//signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	//
	//// Create a gorouting to run the consumer / producer
	//doneCh := make(chan struct{})
	//
	//go func() {
	//	for {
	//		select {
	//		case err := <-consumer.Errors():
	//			fmt.Printf("Consumer error: %s\n", err)
	//		case msg := <-consumer.Messages():
	//			msgCnt++
	//			fmt.Printf("Consumer message: %s with count: %d\n", string(msg.Value), msgCnt)
	//			notification := string(msg.Value)
	//			fmt.Printf("Notification: %s\n", notification)
	//		case <-sigchan:
	//			fmt.Printf("Shutting down\n")
	//			doneCh <- struct{}{}
	//		}
	//	}
	//}()
	//
	//<-doneCh
	//
	//fmt.Printf("Messages processed: %d\n", msgCnt)
	//
	//if err := consumer.Close(); err != nil {
	//	panic(err)
	//}

	topic := "notifications"

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
		wg.Wait()
	}()

	// Runners pool

	runnerPool := runners.NewScenarioPool()

	// Consumer
	//go func() {
	//	err := kafka.StartConsumerGroup(ctx, []string{topic}, kafka.VideoGroup)
	//	if err != nil {
	//		log.Printf("Kafka consumer exited with error: %s\n", err)
	//	}
	//}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartConsumer(ctx, topic, runnerPool)
		if err != nil {
			log.Printf("Kafka consumer exited with error: %s\n", err)
		} else {
			log.Println("Kafka consumer exited successfully")
		}
	}()

	//Producer

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := kafka.StartProducer(ctx)
		if err != nil {
			log.Printf("Kafka producer exited with error: %s\n", err)
		} else {
			log.Println("Kafka producer exited successfully")
		}
	}()

	//if err := kafka.InitKafka(kafka.Brokers); err != nil {
	//	log.Fatal("Failed to init Kafka:", err)
	//}
	//// I may use it somewhere else
	//closeKafka := make(chan struct{})
	//
	//defer func() {
	//	consumerErr, producerErr := kafka.CloseKafka()
	//	if consumerErr != nil {
	//		log.Println("Failed to close Kafka consumer:", consumerErr)
	//	}
	//	if producerErr != nil {
	//		log.Println("Failed to close Kafka producer:", producerErr)
	//	} else if consumerErr == nil && producerErr == nil {
	//		log.Println("Kafka producer and consumer closed")
	//	}
	//}()

	// TODO: Instead of this need to do transactional outbox
	// PostgreSQL initialization
	newClient, err := pclient.NewClient(context.Background())

	if err != nil {
		log.Fatal(err)
		return
	}

	repository := postgresql.NewPgClient(newClient, log.Default())

	// Server init
	client := server.New("8080", repository)

	if err := client.Run(); err != nil {
		return
	}
	
}
