package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer is the demo-side message source used by the app runtime and e2e
// scenarios to create deterministic Kafka traffic.
type Producer struct {
	writer    *kafka.Writer
	sendDelay time.Duration
}

// NewProducer constructs a demo Producer that writes hash-partitioned messages
// to Kafka. ProducerSendDelay controls the pause between successive writes.
func NewProducer(cfg Config) *Producer {
	logger := log.New(log.Writer(), "kafka-producer ", log.LstdFlags)

	sendDelay := cfg.ProducerSendDelay
	if sendDelay <= 0 {
		sendDelay = 100 * time.Millisecond
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.BrokerAddress),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  10,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Logger:       logger,
		ErrorLogger:  logger,
		Transport: &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	return &Producer{
		writer:    writer,
		sendDelay: sendDelay,
	}
}

// ProduceMessages writes numMessages sequentially to Kafka, pausing
// ProducerSendDelay between each write. It stops early if ctx is cancelled.
func (p *Producer) ProduceMessages(ctx context.Context, numMessages int) error {
	log.Printf("Starting to produce %d messages across partitions...", numMessages)

	for i := range numMessages {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("message-%d: Hello from Kafka producer at %s", i, time.Now().Format(time.RFC3339))

		message := kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}

		err := p.writer.WriteMessages(ctx, message)
		if err != nil {
			return fmt.Errorf("failed to write message %d: %w", i, err)
		}

		log.Printf("✓ Produced message %d with key '%s' to partition (hash-distributed)", i, key)
		if err := sleepContext(ctx, p.sendDelay); err != nil {
			return fmt.Errorf("interrupted while waiting between sends: %w", err)
		}
	}

	log.Printf("Successfully produced all %d messages", numMessages)
	return nil
}

// Close flushes any pending writes and releases the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
