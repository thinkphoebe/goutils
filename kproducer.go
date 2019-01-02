package goutils

import (
	"github.com/Shopify/sarama"
)

type KafkaAsyncProducer struct {
	producer sarama.AsyncProducer
}

func (kap *KafkaAsyncProducer) Init(brokeList []string, conf *sarama.Config) error {
	if brokeList == nil || len(brokeList) <= 0 {
	}

	producer, err := sarama.NewAsyncProducer(brokeList, conf)
	if err != nil {
	}

	kap.producer = producer
	return nil
}

func (kap *KafkaAsyncProducer) SendData(topic string, data []byte) error {
	if kap.producer == nil {
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case kap.producer.Input() <- msg:
	case err := <-kap.producer.Errors():
		return err
	}

	return nil
}
