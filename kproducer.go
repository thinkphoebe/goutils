package goutils

import (
	"errors"
	"github.com/Shopify/sarama"
)

type KafkaAsyncProducer struct {
	producer sarama.SyncProducer
}

func (kap *KafkaAsyncProducer) Init(brokeList []string, conf *sarama.Config) error {
	if brokeList == nil || len(brokeList) <= 0 {
		return errors.New("invalid brokerList")
	}

	producer, err := sarama.NewSyncProducer(brokeList, conf)
	if err != nil {
		return err
	}

	kap.producer = producer
	return nil
}

func (kap *KafkaAsyncProducer) SendData(topic string, data []byte) error {
	if kap.producer == nil {
		return errors.New("no producer, you should call Init() first")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	_, _, err := kap.producer.SendMessage(msg)
	return err
}
