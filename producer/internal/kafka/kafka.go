package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
)

type Notification struct {
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func PushNotificationToQueue(topic string, message []byte) error {
	brokers := viper.GetStringSlice("brokers")

	// Create a producer
	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Create a new kafka message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Topic: %s, partition: %d, Offset: %d\n", topic, partition, offset)

	return nil
}
