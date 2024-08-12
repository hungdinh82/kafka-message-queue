package main

import (
    "fmt"
    "log"
    "github.com/IBM/sarama"
    "strconv"
)

func main() {
    // Cấu hình Kafka
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true

    // Tạo một producer
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Failed to start Sarama producer: %v", err)
    }
    defer producer.Close()

    // Gửi nhiều thông điệp
    for i := 1; i <= 5; i++ {
        messageContent := "Hello Kafka " + strconv.Itoa(i)

        // Chỉ định partition cho từng thông điệp (luân phiên giữa các partition)
        partition := int32(i % 2) // Chọn partition 0 hoặc 1

        msg := &sarama.ProducerMessage{
            Topic:     "hung",
            Value:     sarama.StringEncoder(messageContent),
            Partition: partition, // Chỉ định partition
        }

        partition, offset, err := producer.SendMessage(msg)
        if err != nil {
            log.Fatalf("Failed to send message: %v", err)
        }

        fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", msg.Topic, partition, offset)
    }
}
