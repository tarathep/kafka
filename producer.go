package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
)

//Producer send it out as messages to the Kafka cluster
type Producer struct {
	Brokers string
}

//Publish is publish message to kafka
func (p Producer) Publish(topic string, message string) error {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	brokers := []string{p.Brokers}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

//PublishToJSON is Publish message to kafka & convert STRUCT to JSON
func (p Producer) PublishToJSON(topic string, data interface{}) error {
	jsonMsg, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if err := p.Publish(topic, string(jsonMsg)); err != nil {
		return err
	}
	return nil
}
