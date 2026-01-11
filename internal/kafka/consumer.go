package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokerAddress, topic, groupID string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddress},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxAttempts:    10,
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	log.Println("Starting to consume messages from all partitions...")

	for {
		message, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context cancelled, stopping consumer")
				return ctx.Err()
			}
			return fmt.Errorf("failed to read message: %w", err)
		}

		log.Printf("✓ Consumed from partition %d at offset %d: key=%s, value=%s",
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value))
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
