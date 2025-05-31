package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"log"
	"os"
	"time"
	"userapi/internal/cconfig"
	"userapi/internal/domain"
	"userapi/internal/postgresql"
)

type Service struct {
	pg *postgresql.PgClient
}

func init() {
	cconfig.InitConfig()

	Brokers = viper.GetStringSlice("kafka.brokers")
	oldest = viper.GetBool("kafka.oldest")
	verbose = viper.GetBool("kafka.verbose")
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}
	var err error
	version, err = sarama.ParseKafkaVersion(viper.GetString("kafka.version"))
	if err != nil {
		panic(err)
	}

}

var (
	Brokers = []string{""}
	version = sarama.MaxVersion
	oldest  = false
	verbose = true
)

var consumer sarama.Consumer

const VideoGroup = "videos"

//---------------------------------------
// Consumers
//---------------------------------------

func NewRunnerService(service *postgresql.PgClient) *Service {
	return &Service{
		pg: service,
	}
}

func (r *Service) StartConsumer(ctx context.Context, topic string) error {

	config := sarama.NewConfig()
	config.Version = version
	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	var err error

	consumer, err = sarama.NewConsumer(Brokers, config)
	if err != nil {
		log.Panicf("[userapi] Error creating consumer client: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panicf("[userapi] Error from consumer: %v", err)
	}

	log.Println("[userapi] Sarama consumer up and running!...")

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("[userapi] terminating: context cancelled\n")
			break LOOP
		case err = <-partitionConsumer.Errors():
			log.Printf("[userapi] Consumer error: %s\n", err)
		case msg := <-partitionConsumer.Messages():

			var kafkaEvent domain.KafkaEvent

			err = json.Unmarshal(msg.Value, &kafkaEvent)
			if err != nil {
				log.Printf("[userapi] Error unmarshalling kafka event: %v", err)
				continue
			}

			loggableEvent := domain.LoggableKafkaEvent{
				AggregateType: kafkaEvent.AggregateType,
				AggregateID:   kafkaEvent.AggregateID,
				EventType:     kafkaEvent.EventType,
				Payload:       string(kafkaEvent.Payload),
			}

			log.Printf("[userapi] Consumer kafka event: %+v", loggableEvent)

			if err := r.pg.ApplyKafkaEvent(ctx, kafkaEvent); err != nil {
				log.Printf("[userapi] Error applying kafka event: %v", err)
			}

		}
	}

	if err = partitionConsumer.Close(); err != nil {
		log.Printf("[userapi] Error closing consumer: %v", err)
		panic(err)
	}

	return nil
}

//---------------------------------------
// Producers
//---------------------------------------

var kafkaProducer sarama.AsyncProducer

func StartProducer(ctx context.Context) error {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.MaxMessageBytes = 1000000

	var err error

	kafkaProducer, err = sarama.NewAsyncProducer(Brokers, config)

	if err != nil {
		log.Fatal("[userapi] Failed to start Kafka producer:", err)
		return err
	}

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Println("[userapi] terminating: context cancelled")
			break LOOP
		case msg, ok := <-kafkaProducer.Errors():
			if !ok {
				return msg
			}
			log.Println(msg.Err)
		case msg, ok := <-kafkaProducer.Successes():
			if !ok {
				return fmt.Errorf("error reading from producer success channel: %v", err)
			}
			log.Println(fmt.Sprintf("[userapi] Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}

	err = kafkaProducer.Close()
	if err != nil {
		log.Printf("[userapi] error closing producer: %v", err)
	}

	return err
}

func PushMessageToQueue(msg *sarama.ProducerMessage) {

	if kafkaProducer == nil {
		log.Print("[userapi] kafka producer is not initialized")
		return
	}

	kafkaProducer.Input() <- msg
}

// Runners

func ChangeRunnerState(runnerAction domain.RunnerMsg) {
	topic := "scenario"
	message, _ := json.Marshal(runnerAction)

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(message),
		Partition: 0,
	}
	PushMessageToQueue(msg)
}
