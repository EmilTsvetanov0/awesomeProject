package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"time"
)

var kafkaProducer sarama.AsyncProducer

func InitKafka(brokers []string) error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.MaxMessageBytes = 1000000

	var err error

	kafkaProducer, err = sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		log.Fatal("Failed to init Kafka producer:", err)
		return err
	}

	go handleAsyncProducerEvents()

	return err
}

func handleAsyncProducerEvents() {
	for {
		select {
		case msg, ok := <-kafkaProducer.Errors():
			if !ok {
				return
			}
			log.Println(msg.Err)
		case msg, ok := <-kafkaProducer.Successes():
			if !ok {
				return
			}
			log.Println(fmt.Sprintf("Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}
}

func CloseKafka() error {
	return kafkaProducer.Close()
}

type Notification struct {
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

//func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
//	config := sarama.NewConfig()
//	config.Producer.RequiredAcks = sarama.WaitForAll
//	config.Producer.Return.Successes = true
//	config.Producer.Return.Errors = true
//	config.Producer.Retry.Max = 5
//
//	return sarama.NewSyncProducer(brokers, config)
//}

func PushNotificationToQueue(topic string, message []byte) {

	if kafkaProducer == nil {
		log.Print("kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	kafkaProducer.Input() <- msg
}
