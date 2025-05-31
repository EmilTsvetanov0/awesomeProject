package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"log"
	"os"
	"producer/internal/cconfig"
	"producer/internal/domain"
	"time"
)

type RunnerPool interface {
	Start(ctx context.Context, Id string)
	Stop(id string)
}

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

const videoTopic = "videos"

// Consumer

type runnerGroupHandler struct {
	runnerPool RunnerPool
}

// TODO: Mock, need to replace this with maybe database records
var availablePaths = map[string]bool{
	"first":  true,
	"second": true,
	"krol":   true,
}

func (h *runnerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *runnerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *runnerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var runnerMsg domain.RunnerMsg
		if err := json.Unmarshal(msg.Value, &runnerMsg); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		if active, ok := availablePaths[runnerMsg.Id]; !(active && ok) {
			if !ok {
				log.Printf("Runner %s is not found in available paths", runnerMsg.Id)
			} else {
				log.Printf("Runner %s is not active", runnerMsg.Id)
			}
			continue
		}

		log.Printf("Message: %+v", runnerMsg)
		switch runnerMsg.Action {
		case "start":
			h.runnerPool.Start(context.Background(), runnerMsg.Id)
		case "stop":
			h.runnerPool.Stop(runnerMsg.Id)
		}

		// отмечаем сообщение как обработанное
		sess.MarkMessage(msg, "")
	}
	return nil
}

// Новый метод запуска ConsumerGroup
func StartConsumerGroup(ctx context.Context, topic, groupID string, rp RunnerPool) error {
	config := sarama.NewConfig()
	config.Version = version
	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	// стратегия разбаланса
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// создаём группу
	group, err := sarama.NewConsumerGroup(Brokers, groupID, config)
	if err != nil {
		return fmt.Errorf("[producer] error creating consumer group: %w", err)
	}
	defer group.Close()

	handler := &runnerGroupHandler{runnerPool: rp}

	// логируем ошибки группы
	go func() {
		for err := range group.Errors() {
			log.Printf("[producer] Consumer group error: %v", err)
		}
	}()

	// запускаем цикл Consume
	for {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("[producer] Error during Consume: %v", err)
			time.Sleep(time.Second)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// Producer

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
			log.Println(fmt.Sprintf("[producer] Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
			bytes, err := msg.Value.Encode()
			if err != nil {
				log.Printf("Error decoding message value: %v", err)
			} else {
				log.Printf("[producer] Topic: %s, Msg: %s", msg.Topic, string(bytes))
			}
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

type RunnerMsg struct {
	Id     string `json:"id"`
	Action string `json:"action"`
}

func PushImageToQueue(id string, message []byte) {

	if kafkaProducer == nil {
		log.Print("[producer] [PushImageToQueue] kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: videoTopic,
		Value: sarama.ByteEncoder(message),
		Key:   sarama.StringEncoder(id),
	}

	kafkaProducer.Input() <- msg
}
