package sarama_kafka

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var (
	kAsyncProducer               sarama.AsyncProducer
	kClient                      sarama.Client
	kAsyncProducerMessagePending chan *sarama.ProducerMessage
)

const (
	ASYNC_PENDING_LENGTH = 65535
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
	kAsyncProducerMessagePending = make(chan *sarama.ProducerMessage, ASYNC_PENDING_LENGTH)

	go asyncInput()
}

func asyncInput() {
	for {
		select {
		case msg, ok := <-kAsyncProducerMessagePending:
			if !ok {
				log.Error("Fail to get pending message from chan")
				return
			}

			kAsyncProducer.Input() <- msg
		}
	}
}

func enqueue(msg *sarama.ProducerMessage) {
	select {
	case kAsyncProducerMessagePending <- msg:
	default:
		log.Warn("kafka asyncProducer chan is full")
	}
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
