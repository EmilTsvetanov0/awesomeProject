package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"log"
	"os"
	"outbox/internal/cconfig"
	"outbox/internal/domain"
	"outbox/internal/postgresql"
	"time"
)

type RunnerService struct {
	pg *postgresql.PgClient
}

func NewRunnerService(pg *postgresql.PgClient) *RunnerService {
	return &RunnerService{pg}
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
	Brokers            = []string{""}
	version            = sarama.MaxVersion
	oldest             = false
	verbose            = true
	outboxReadInterval = 2 * time.Second
	maxBatchSize       = 100
)

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
		log.Fatal("[outbox] Failed to start Kafka producer:", err)
		return err
	}

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Println("[outbox] terminating: context cancelled")
			break LOOP
		case msg, ok := <-kafkaProducer.Errors():
			if !ok {
				return msg
			}
			log.Println(msg.Err)
		case msg, ok := <-kafkaProducer.Successes():
			if !ok {
				return fmt.Errorf("[outbox] error reading from producer success channel: %v", err)
			}
			log.Println(fmt.Sprintf("[outbox] Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}

	err = kafkaProducer.Close()
	if err != nil {
		log.Printf("[outbox] error closing producer: %v", err)
	}

	return err
}

// Run only after the producer
func (r *RunnerService) StartOutboxDispatcher(ctx context.Context) {
	ticker := time.NewTicker(outboxReadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[outbox] Shutting down dispatcher...")
			return

		case <-ticker.C:
			events, err := r.pg.GetUnprocessedOutboxEvents(ctx, maxBatchSize)
			if err != nil {
				log.Printf("[outbox] error reading events: %v", err)
				continue
			}

			for _, evt := range events {
				SendToOutbox(evt)

				err := r.pg.MarkEventProcessed(ctx, evt.ID)
				if err != nil {
					log.Printf("[outbox] failed to mark event %s as processed: %v", evt.ID, err)
				}
			}
		}
	}
}

func PushMessageToQueue(topic string, key sarama.Encoder, message []byte) {

	if kafkaProducer == nil {
		log.Print("[outbox] kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   key,
	}

	kafkaProducer.Input() <- msg
}

func SendToOutbox(evt domain.KafkaEvent) {
	topic := "outbox"
	message, _ := json.Marshal(evt)
	PushMessageToQueue(topic, sarama.StringEncoder(""), message)
}
