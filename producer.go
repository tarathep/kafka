package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
)

// Producer Kafka by "github.com/Shopify/sarama"
type Producer struct{}

//Publish is publish message to kafka
func (Producer) Publish(brokerAddress string, topic string, message string) error {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	brokers := []string{brokerAddress}
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

//PublishToJSON is Publish into kafka (convert STRUCT to JSON)
func (p Producer) PublishToJSON(brokerAddress string, topic string, data interface{}) error {
	jsonMsg, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if err := p.Publish(brokerAddress, topic, string(jsonMsg)); err != nil {
		return err
	}
	return nil
}
