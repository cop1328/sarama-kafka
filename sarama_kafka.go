package sarama_kafka

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var (
	kAsyncProducer sarama.AsyncProducer
	kClient        sarama.Client
)

func Init(brokers []string) {
	if len(brokers) == 0 {
		log.Fatal("kafka is brokers not configed")
		return
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	kAsyncProducer = producer
	cli, err := sarama.NewClient(brokers, nil)
	if err != nil {
		log.Fatal(err)
	}

	kClient = cli
}

func enqueue(msg *sarama.ProducerMessage) {
	kAsyncProducer.Input() <- msg
}

func Submit(topic string, value interface{}) {
	if bts, err := json.Marshal(&value); err == nil {
		enqueue(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(bts),
		})
	} else {
		log.Error(err)
	}
}

func SubmitWithKey(topic, key string, value interface{}) {
	if bts, err := json.Marshal(&value); err == nil {
		enqueue(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(bts),
		})
	} else {
		log.Error(err)
	}
}

func NewConsumer() (sarama.Consumer, error) {
	return sarama.NewConsumerFromClient(kClient)
}
