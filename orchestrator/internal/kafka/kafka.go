package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"log"
	"orchestrator/internal/cconfig"
	"orchestrator/internal/domain"
	"os"
	"time"
)

type RunnerManager interface {
	RunScenario(ctx context.Context, id string) error
	StopScenario(ctx context.Context, id string) error
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
	oldest  = false // Это чтобы перечитывать партиции каждый раз с начала
	verbose = true
)

var consumer sarama.Consumer

const VideoGroup = "videos"

//---------------------------------------
// Consumers
//---------------------------------------

func StartConsumer(ctx context.Context, topic string, r RunnerManager) error {

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
		log.Panicf("Error creating consumer client: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panicf("Error from consumer: %v", err)
	}

	log.Println("Sarama consumer up and running!...")

	msgCnt := 0

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("terminating: context cancelled\n")
			break LOOP
		case err = <-partitionConsumer.Errors():
			log.Printf("Consumer error: %s\n", err)
		case msg := <-partitionConsumer.Messages():
			msgCnt++

			var runnerMsg domain.RunnerMsg

			err = json.Unmarshal(msg.Value, &runnerMsg)
			if err != nil {
				log.Printf("Error unmarshalling message: %v", err)
				continue
			}

			log.Printf("Consumer message: %+v with count: %d\n", runnerMsg, msgCnt)
			if runnerMsg.Action == "start" {
				if err = r.RunScenario(ctx, runnerMsg.Id); err != nil {
					log.Printf("Error running scenario: %v", err)
				}
			} else if runnerMsg.Action == "stop" {
				if err = r.StopScenario(ctx, runnerMsg.Id); err != nil {
					log.Printf("Error running scenario: %v", err)
				}
			}

		}
	}

	if err = partitionConsumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
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
		log.Fatal("Failed to start Kafka producer:", err)
		return err
	}

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
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
			log.Println(fmt.Sprintf("Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}

	err = kafkaProducer.Close()
	if err != nil {
		log.Printf("error closing producer: %v", err)
	}

	return err
}

func PushMessageToQueue(topic string, key sarama.Encoder, message []byte) {

	if kafkaProducer == nil {
		log.Print("kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   key,
	}

	kafkaProducer.Input() <- msg
}

// Runners

func StartRunner(id string) {
	topic := "runners"
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "start"})
	PushMessageToQueue(topic, sarama.StringEncoder(id), message)
}

func StopRunner(id string) {
	topic := "runners"
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "stop"})
	PushMessageToQueue(topic, sarama.StringEncoder(id), message)
}

func ChangeState(id string, state string) {
	topic := "outbox"
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: state})
	PushMessageToQueue(topic, sarama.StringEncoder(id), message)
}
