package main

import (
    "fmt"
    "log"
    "github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true

    consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Failed to start Sarama consumer: %v", err)
    }
    defer consumer.Close()

    partitionConsumer, err := consumer.ConsumePartition("hung", 1, sarama.OffsetNewest)
    if err != nil {
        log.Fatalf("Failed to start partition consumer: %v", err)
    }
    defer partitionConsumer.Close()

    for msg := range partitionConsumer.Messages() {
        fmt.Printf("Consumer2 received message: %s\n", string(msg.Value))
    }
}
