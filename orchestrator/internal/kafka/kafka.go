package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/spf13/viper"
	"hash/fnv"
	"log"
	"math/rand"
	"orchestrator/internal/cconfig"
	"orchestrator/internal/domain"
	"os"
	"time"
)

type ScenarioManager interface {
	HandleScenario(ctx context.Context, runnerMsg *domain.RunnerMsg)
	FinishWorkers()
	WaitForWorkers()
}

type Consumer struct {
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	ScenarioManager
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

	tmpConfig := sarama.NewConfig()
	tmpConfig.Version = version

	tmpClient, err := sarama.NewClient(Brokers, tmpConfig)
	if err != nil {
		log.Fatal("[orchestrator] Ошибка создания клиента kafka:", err)
	}
	defer tmpClient.Close()

	runnersTopicPartitions, err = tmpClient.Partitions(runnersTopic)
	log.Printf("RunnerTopicPartitions: %v", runnersTopicPartitions)

}

var (
	Brokers                = []string{""}
	version                = sarama.MaxVersion
	oldest                 = false // Это чтобы перечитывать партиции каждый раз с начала
	verbose                = true
	runnersTopicPartitions = make([]int32, 0) //Это для broadcast'а по всем партициям для остановки сценария
)

const VideoGroup = "videos"
const runnersTopic = "runners"

//---------------------------------------
// Consumers
//---------------------------------------

func NewKafkaConsumer(sc ScenarioManager) (*Consumer, error) {

	config := sarama.NewConfig()
	config.Version = version
	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	cons, err := sarama.NewConsumer(Brokers, config)
	if err != nil {
		return &Consumer{}, fmt.Errorf("[orchestrator] Error creating consumer client: %v", err)
	}

	return &Consumer{
		consumer:        cons,
		ScenarioManager: sc,
	}, nil
}

func (r *Consumer) StartConsumer(ctx context.Context, topic string) error {

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error

	r.partitionConsumer, err = r.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("[orchestrator] Error from consumer: %v", err)
	}

	log.Println("[orchestrator] Sarama consumer up and running!...")

	msgCnt := 0

LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Printf("[orchestrator] terminating: context cancelled\n")
			cancel()
			break LOOP
		case err = <-r.partitionConsumer.Errors():
			log.Printf("[orchestrator] Consumer error: %s\n", err)
		case msg := <-r.partitionConsumer.Messages():
			msgCnt++

			var runnerMsg domain.RunnerMsg

			err = json.Unmarshal(msg.Value, &runnerMsg)
			if err != nil {
				log.Printf("[orchestrator] Error unmarshalling message: %v", err)
				continue
			}

			log.Printf("[orchestrator] Consumer message: %+v with count: %d\n", runnerMsg, msgCnt)

			r.HandleScenario(childCtx, &runnerMsg)

		}
	}

	r.FinishWorkers()

	if err = r.partitionConsumer.Close(); err != nil {
		log.Printf("[orchestrator] Error closing consumer: %v", err)
		panic(err)
	}

	r.WaitForWorkers()

	return nil
}

//---------------------------------------
// Producers
//---------------------------------------

type Producer struct {
	producer sarama.AsyncProducer
}

func NewCustomPartitioner(topic string) sarama.Partitioner {
	return &customPartitioner{}
}

type customPartitioner struct{}

func (p *customPartitioner) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if msg.Partition >= 0 {
		return msg.Partition % numPartitions, nil
	}
	if msg.Key == nil {
		return rand.Int31n(numPartitions), nil
	}

	hasher := fnv.New32a()
	keyBytes, err := msg.Key.Encode()
	if err != nil {
		return rand.Int31n(numPartitions), nil
	}
	_, _ = hasher.Write(keyBytes)

	partition := int32(hasher.Sum32() % uint32(numPartitions))
	return partition, nil
}

func (p *customPartitioner) RequiresConsistency() bool {
	return true
}

func NewKafkaProducer() (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = NewCustomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Messages = 1000
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.MaxMessageBytes = 1000000

	producer, err := sarama.NewAsyncProducer(Brokers, config)

	if err != nil {
		return &Producer{}, err
	}
	return &Producer{producer: producer}, nil
}

func (p *Producer) StartProducer(ctx context.Context) error {
LOOP:
	for {
		select {
		case <-ctx.Done():
			log.Println("[orchestrator] terminating: context cancelled")
			break LOOP
		case msg, ok := <-p.producer.Errors():
			if !ok {
				return msg
			}
			log.Println(msg.Err)
		case msg, ok := <-p.producer.Successes():
			if !ok {
				return fmt.Errorf("error reading from producer success channel")
			}
			log.Println(fmt.Sprintf("[orchestrator] Topic: %s, partition: %d, Offset: %d, Timestamp: %s", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format(time.RFC3339)))
		}
	}

	err := p.producer.Close()
	if err != nil {
		log.Printf("[orchestrator] error closing producer: %v", err)
	}

	return err
}

func (p *Producer) PushMessageToQueueKey(topic string, key sarama.Encoder, message []byte) {

	if p.producer == nil {
		log.Print("[orchestrator] kafka producer is not initialized")
		return
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(message),
		Key:       key,
		Partition: -1,
	}

	p.producer.Input() <- msg
}

func (p *Producer) PushMessageToQueue(msg *sarama.ProducerMessage) {

	if p.producer == nil {
		log.Print("[orchestrator] kafka producer is not initialized")
		return
	}

	p.producer.Input() <- msg
}

// Runners

func (p *Producer) StartRunner(id string) {
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "start"})
	p.PushMessageToQueueKey(runnersTopic, sarama.StringEncoder(id), message)
}

func (p *Producer) StopRunner(id string) {
	log.Println("[orchestrator] [StopRunner] Stopping runner " + id)
	message, _ := json.Marshal(domain.RunnerMsg{Id: id, Action: "stop"})

	// В связи с непредсказуемой ребалансировкой, единственный рабочий вариант - отправлять уведы об остановке всем консьюмерам сразу
	for _, partition := range runnersTopicPartitions {
		msg := &sarama.ProducerMessage{
			Topic:     runnersTopic,
			Value:     sarama.ByteEncoder(message),
			Partition: partition,
		}
		p.PushMessageToQueue(msg)
	}
}
