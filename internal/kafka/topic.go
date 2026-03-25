package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func RecreateTopic(ctx context.Context, cfg Config) error {
	log.Printf("Recreating topic '%s' with %d partitions...", cfg.Topic, cfg.Partitions)

	adminAddress := cfg.ControllerAddress
	if adminAddress == "" {
		adminAddress = cfg.BrokerAddress
	}

	if err := deleteTopicWithRetry(ctx, adminAddress, cfg.Topic); err != nil {
		return err
	}

	if err := createTopicWithRetry(ctx, adminAddress, cfg.Topic, cfg.Partitions); err != nil {
		return err
	}
	return nil
}

func deleteTopicWithRetry(ctx context.Context, controllerAddress, topic string) error {
	log.Printf("Deleting topic '%s' if it exists...", topic)

	controllerConn, err := dialWithRetry(ctx, controllerAddress)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.DeleteTopics(topic)
	if err != nil {
		// Treat these as "topic doesn't exist" - safe to continue
		if strings.Contains(err.Error(), "does not exist") ||
			strings.Contains(err.Error(), "Unknown topic") ||
			strings.Contains(err.Error(), "multiple Read calls") {
			log.Printf("Topic '%s' does not exist or broker not ready (nothing to delete)", topic)
			return nil
		}
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	log.Printf("✓ Deleted topic '%s'", topic)
	return nil
}

func dialWithRetry(ctx context.Context, address string) (*kafka.Conn, error) {
	var conn *kafka.Conn
	var err error

	for attempt := 1; attempt <= 10; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err = kafka.DialContext(dialCtx, "tcp", address)
		cancel()

		if err == nil {
			return conn, nil
		}

		log.Printf("Attempt %d: Failed to connect to Kafka broker at %s: %v (retrying in 2s...)", attempt, address, err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	return nil, fmt.Errorf("failed to connect to Kafka broker after 10 attempts: %w", err)
}

func createTopicWithRetry(ctx context.Context, controllerAddress, topic string, numPartitions int) error {
	log.Printf("Creating topic '%s' with %d partitions...", topic, numPartitions)

	topicConfig := kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1, // Single broker setup
	}

	// Retry topic creation - KRaft controller may need more time
	var (
		err            error
		controllerConn *kafka.Conn
	)
	for attempt := 1; attempt <= 10; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Create a fresh connection for each attempt to avoid broken pipe errors.
		// Use = (not :=) to update the outer err so the final return can report
		// the last failure if all retries are exhausted.
		controllerConn, err = dialWithRetry(ctx, controllerAddress)
		if err != nil {
			return fmt.Errorf("failed to connect to broker: %w", err)
		}

		err = controllerConn.CreateTopics(topicConfig)
		controllerConn.Close() // Close connection after each attempt

		if err == nil {
			log.Printf("✓ Created topic '%s' with %d partitions", topic, numPartitions)
			return nil
		}

		// If it's "already exists", that's fine
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("Topic '%s' already exists", topic)
			return nil
		}

		// If it's a retriable error, wait and retry
		if strings.Contains(err.Error(), "multiple Read calls") ||
			strings.Contains(err.Error(), "connection refused") ||
			strings.Contains(err.Error(), "broken pipe") {
			log.Printf("Attempt %d: Broker not ready yet (%v), retrying in 2s...", attempt, err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}

		return fmt.Errorf("failed to create topic: %w", err)
	}

	return fmt.Errorf("failed to create topic after %d retries: %w", 10, err)
}
