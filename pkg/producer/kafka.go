package producer

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	_ "github.com/confluentinc/confluent-kafka-go/kafka/librdkafka_vendor"
)

// Kafka interface for kafka producer methods
type Kafka interface {
	KProduce(msg interface{})
	KFlushLoop()
	KFlushOnce()
	DeliveryReports()
}

// KafkaService implements kafka interface
type KafkaService struct {
	p     *kafka.Producer
	topic string
}

// NewKafkaService creates a new instance for KafkaService
// this is a kafka producer
func NewKafkaService(topic string, configMap *kafka.ConfigMap) (*KafkaService, error) {
	p, err := kafka.NewProducer(configMap)
	return &KafkaService{p: p, topic: topic}, err
}

// KProduce json marshals the input and sends the value on the configured kafka topic
func (k *KafkaService) KProduce(msg []byte) {
	err := k.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Value:          msg,
	}, nil)

	if err != nil {
		log.Printf("kafka produce error %v\n", err)
	}
}

// KFlushLoop calls the kafka Flush method in an infinite loop
func (k *KafkaService) KFlushLoop() {
	// NOTE: flush blocks producer. This should be used sparingly
	for {
		k.p.Flush(1 * 1000)
		time.Sleep(120 * time.Second)
	}
}

// KFlushOnce calls the kafka Flush method once
func (k *KafkaService) KFlushOnce() {
	k.p.Flush(10000)
}

// Shutdown closes the kafka connection
func (k *KafkaService) Shutdown() {
	k.p.Close()
}

// DeliveryReports listens for kafka events
func (k *KafkaService) DeliveryReports() {
	for e := range k.p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				log.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}
