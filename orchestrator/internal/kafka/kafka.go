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
	"orchestrator/internal/postgresql"
	"os"
	"time"
)

type ScenarioManager interface {
	ScenarioExists(id string) bool
	RunScenario(ctx context.Context, id string) error
	StopScenario(ctx context.Context, id string) error
}

type RunnerService struct {
	ScenarioManager
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
	oldest  = false // Это чтобы перечитывать партиции каждый раз с начала
	verbose = true
)

var consumer sarama.Consumer

const VideoGroup = "videos"

//---------------------------------------
// Consumers
//---------------------------------------

func NewRunnerService(sc ScenarioManager, service *postgresql.PgClient) *RunnerService {
	return &RunnerService{
		ScenarioManager: sc,
		pg:              service,
	}
}

func (r *RunnerService) StartConsumer(ctx context.Context, topic string) error {

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
		log.Panicf("[orchestrator] Error creating consumer client: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Panicf("[orchestrator] Error from consumer: %v", err)
	}

	log.Println("[orchestrator] Sarama consumer up and running!...")

	msgCnt := 0

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("[orchestrator] terminating: context cancelled\n")
			break LOOP
		case err = <-partitionConsumer.Errors():
			log.Printf("[orchestrator] Consumer error: %s\n", err)
		case msg := <-partitionConsumer.Messages():
			msgCnt++

			var runnerMsg domain.RunnerMsg

			err = json.Unmarshal(msg.Value, &runnerMsg)
			if err != nil {
				log.Printf("[orchestrator] Error unmarshalling message: %v", err)
				continue
			}

			log.Printf("[orchestrator] Consumer message: %+v with count: %d\n", runnerMsg, msgCnt)
			if runnerMsg.Action == "start" {
				if !r.ScenarioExists(runnerMsg.Id) {
					log.Printf("[orchestrator] Scenario %s does not exist. Creatingif s.runnerPool == nil {\n\t\tlog.Println(\"❌ runnerPool is nil\")\n\t\tc.JSON(http.StatusInternalServerError, gin.H{\"error\": \"runnerPool is nil\"})\n\t\treturn\n\t}...", runnerMsg.Id)
					err = r.pg.InsertScenario(ctx, runnerMsg.Id)
					if err != nil {
						log.Printf("[orchestrator] Error creating scenario: %v", err)
					}
				}
				log.Printf("Starting scenario %s", runnerMsg.Id)
				if err = r.RunScenario(ctx, runnerMsg.Id); err != nil {
					log.Printf("[orchestrator] Error running scenario: %v", err)
				}
			} else if runnerMsg.Action == "stop" {
				if err = r.StopScenario(ctx, runnerMsg.Id); err != nil {
					log.Printf("[orchestrator] Error running scenario: %v", err)
				}
			} else {
				log.Printf("[orchestrator] Skipping scenario %s, command \"%s\" is not recognized", runnerMsg.Id, runnerMsg.Action)
			}

		}
	}

	if err = partitionConsumer.Close(); err != nil {
		log.Printf("[orchestrator] Error closing consumer: %v", err)
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
		log.Fatal("[orchestrator] Failed to start Kafka producer:", err)
		return err
	}

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Println("[orchestrator] terminating: context cancelled")
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
			log.Println(fmt.Sprintf("[orchestrator] Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}

	err = kafkaProducer.Close()
	if err != nil {
		log.Printf("[orchestrator] error closing producer: %v", err)
	}

	return err
}

func PushMessageToQueueKey(topic string, key sarama.Encoder, message []byte) {

	if kafkaProducer == nil {
		log.Print("[orchestrator] kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   key,
	}

	kafkaProducer.Input() <- msg
}

func PushMessageToQueue(msg *sarama.ProducerMessage) {

	if kafkaProducer == nil {
		log.Print("[orchestrator] kafka producer is not initialized")
		return
	}

	kafkaProducer.Input() <- msg
}

// Runners

func StartRunner(id string) {
	topic := "runners"
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "start"})
	PushMessageToQueueKey(topic, sarama.StringEncoder(id), message)
}

func StopRunner(id string) {
	log.Println("[orchestrator] [StopRunner] Stopping runner " + id)
	topic := "runners"
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "stop"})

	// В связи с непредсказуемой ребалансировкой, единственный рабочий вариант - отправлять уведы об остановке всем консьюмерам сразу
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("[orchestrator] [StopRunner] Error getting list of partitions for topic %s: %v", topic, err)
	}

	for _, partition := range partitions {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(message),
			Partition: partition,
		}
		PushMessageToQueue(msg)
	}
}
