package kafka

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

//Consumer that will dump out messages from Kafka
type Consumer struct {
	Brokers string
	Topic   string
}

//Subscribe is listen message from kafka
func (c Consumer) Subscribe(onChangeListener func(Property)) {
	// create a property with initial value
	prop := NewProperty("")

	// run 10 observers
	go onChangeListener(prop)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Specify brokers address. This is default one
	brokers := []string{c.Brokers}

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
	consumer, err := master.ConsumePartition(c.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})

	// run one publisher
	val := prop.Value().(string)

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				//fmt.Println("Received messages" + string(msg.Key) + " VALUE ==>" + string(msg.Value))
				// update property
				val = string(msg.Value)
				prop.Update(val)

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
