package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

type Config struct {
	Kafka kafka.Config
	App   AppConfig
}

// AppConfig keeps top-level demo inputs separate from Kafka-specific tuning.
// For now the only app-level dial is how many demo messages the producer writes.
type AppConfig struct {
	NumMessages int
}

// Validate checks both Kafka-side and App-side invariants. It is called
// automatically by Load and should be called on any Config constructed manually
// (e.g. in tests or e2e scenarios).
func (c *Config) Validate() error {
	if c.App.NumMessages < 1 {
		return fmt.Errorf("config: NumMessages must be >= 1, got %d", c.App.NumMessages)
	}
	if err := c.Kafka.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// Load builds a Config from compiled defaults, then applies any environment
// variable overrides. The caller is responsible for calling Validate() on the
// result — Runtime.Run() does this automatically, but manually constructed
// pipelines (e.g. e2e scenarios) should validate explicitly.
func Load() *Config {
	cfg := &Config{
		Kafka: kafka.Config{
			BrokerAddress:     fmt.Sprintf("127.0.0.1:%s", docker.KafkaPlaintextPort),
			ControllerAddress: fmt.Sprintf("127.0.0.1:%s", docker.KafkaControllerPort),
			Topic:             "test-topic",
			Partitions:        3,
			GroupID:           "test-consumer-group",
			WorkerCount:       2,
			QueueCapacity:     32,
			MaxRetryAttempts:  3,
			RetryBaseDelay:    100 * time.Millisecond,
			RetryMaxDelay:     time.Second,
			CommitTimeout:     5 * time.Second,
			DrainTimeout:      5 * time.Second,
			QuiescenceWindow:  250 * time.Millisecond,
			ProducerSendDelay: 100 * time.Millisecond,
		},
		App: AppConfig{
			NumMessages: 10,
		},
	}

	applyEnvOverrides(cfg)

	return cfg
}

// applyEnvOverrides reads well-known environment variables and overwrites the
// corresponding Config fields. Only non-empty values are applied; unparseable
// values are silently ignored so that the subsequent Validate call can catch
// structural problems instead.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KAFKA_BROKER_ADDRESS"); v != "" {
		cfg.Kafka.BrokerAddress = v
	}
	if v := os.Getenv("KAFKA_CONTROLLER_ADDRESS"); v != "" {
		cfg.Kafka.ControllerAddress = v
	}
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		cfg.Kafka.Topic = v
	}
	if v := os.Getenv("KAFKA_GROUP_ID"); v != "" {
		cfg.Kafka.GroupID = v
	}
	if v, err := strconv.Atoi(os.Getenv("KAFKA_PARTITIONS")); err == nil && v > 0 {
		cfg.Kafka.Partitions = v
	}
	if v, err := strconv.Atoi(os.Getenv("KAFKA_WORKER_COUNT")); err == nil && v > 0 {
		cfg.Kafka.WorkerCount = v
	}
	if v, err := strconv.Atoi(os.Getenv("KAFKA_QUEUE_CAPACITY")); err == nil && v >= 0 {
		cfg.Kafka.QueueCapacity = v
	}
	if v, err := strconv.Atoi(os.Getenv("KAFKA_MAX_RETRY_ATTEMPTS")); err == nil && v >= 0 {
		cfg.Kafka.MaxRetryAttempts = v
	}
	if v, err := time.ParseDuration(os.Getenv("KAFKA_RETRY_BASE_DELAY")); err == nil {
		cfg.Kafka.RetryBaseDelay = v
	}
	if v, err := time.ParseDuration(os.Getenv("KAFKA_RETRY_MAX_DELAY")); err == nil {
		cfg.Kafka.RetryMaxDelay = v
	}
	if v, err := time.ParseDuration(os.Getenv("KAFKA_COMMIT_TIMEOUT")); err == nil {
		cfg.Kafka.CommitTimeout = v
	}
	if v, err := time.ParseDuration(os.Getenv("KAFKA_DRAIN_TIMEOUT")); err == nil {
		cfg.Kafka.DrainTimeout = v
	}
	if v, err := strconv.Atoi(os.Getenv("APP_NUM_MESSAGES")); err == nil && v > 0 {
		cfg.App.NumMessages = v
	}
}
