package kafka

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

//Consumer is ..
type Consumer struct{}

func Subscribe(brokerAddress string, topic string) {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	brokers := []string{brokerAddress}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages" + string(msg.Key) + " VALUE ==>" + string(msg.Value))

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}

		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
