package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadReturnsValidConfig(t *testing.T) {
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Load() returned invalid config: %v", err)
	}
}

func TestValidateRejectsEmptyBrokerAddress(t *testing.T) {
	cfg := Load()
	cfg.Kafka.BrokerAddress = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for empty BrokerAddress")
	}
}

func TestValidateRejectsEmptyTopic(t *testing.T) {
	cfg := Load()
	cfg.Kafka.Topic = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for empty Topic")
	}
}

func TestValidateRejectsEmptyGroupID(t *testing.T) {
	cfg := Load()
	cfg.Kafka.GroupID = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for empty GroupID")
	}
}

func TestValidateRejectsZeroPartitions(t *testing.T) {
	cfg := Load()
	cfg.Kafka.Partitions = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for zero Partitions")
	}
}

func TestValidateRejectsZeroWorkerCount(t *testing.T) {
	cfg := Load()
	cfg.Kafka.WorkerCount = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for zero WorkerCount")
	}
}

func TestValidateRejectsRetryMaxDelayBelowBaseDelay(t *testing.T) {
	cfg := Load()
	cfg.Kafka.RetryBaseDelay = time.Second
	cfg.Kafka.RetryMaxDelay = time.Millisecond
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for RetryMaxDelay < RetryBaseDelay")
	}
}

func TestValidateRejectsZeroNumMessages(t *testing.T) {
	cfg := Load()
	cfg.App.NumMessages = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() expected error for zero NumMessages")
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error = %v", err)
	}
}

func TestEnvOverrideBrokerAddress(t *testing.T) {
	t.Setenv("KAFKA_BROKER_ADDRESS", "custom-broker:9092")
	cfg := Load()
	if cfg.Kafka.BrokerAddress != "custom-broker:9092" {
		t.Fatalf("BrokerAddress = %q, want %q", cfg.Kafka.BrokerAddress, "custom-broker:9092")
	}
}

func TestEnvOverrideTopic(t *testing.T) {
	t.Setenv("KAFKA_TOPIC", "custom-topic")
	cfg := Load()
	if cfg.Kafka.Topic != "custom-topic" {
		t.Fatalf("Topic = %q, want %q", cfg.Kafka.Topic, "custom-topic")
	}
}

func TestEnvOverrideGroupID(t *testing.T) {
	t.Setenv("KAFKA_GROUP_ID", "custom-group")
	cfg := Load()
	if cfg.Kafka.GroupID != "custom-group" {
		t.Fatalf("GroupID = %q, want %q", cfg.Kafka.GroupID, "custom-group")
	}
}

func TestEnvOverrideWorkerCount(t *testing.T) {
	t.Setenv("KAFKA_WORKER_COUNT", "8")
	cfg := Load()
	if cfg.Kafka.WorkerCount != 8 {
		t.Fatalf("WorkerCount = %d, want 8", cfg.Kafka.WorkerCount)
	}
}

func TestEnvOverrideNumMessages(t *testing.T) {
	t.Setenv("APP_NUM_MESSAGES", "42")
	cfg := Load()
	if cfg.App.NumMessages != 42 {
		t.Fatalf("NumMessages = %d, want 42", cfg.App.NumMessages)
	}
}

func TestEnvOverrideInvalidIntIgnored(t *testing.T) {
	t.Setenv("KAFKA_WORKER_COUNT", "not-a-number")
	cfg := Load()
	if cfg.Kafka.WorkerCount != 2 { // default
		t.Fatalf("WorkerCount = %d, want default 2 after invalid env", cfg.Kafka.WorkerCount)
	}
}

func TestEnvOverrideRetryBaseDelay(t *testing.T) {
	t.Setenv("KAFKA_RETRY_BASE_DELAY", "500ms")
	cfg := Load()
	if cfg.Kafka.RetryBaseDelay != 500*time.Millisecond {
		t.Fatalf("RetryBaseDelay = %v, want 500ms", cfg.Kafka.RetryBaseDelay)
	}
}

func TestEnvOverrideEmptyPreservesDefault(t *testing.T) {
	// Explicitly clear env vars to verify defaults are kept.
	os.Unsetenv("KAFKA_BROKER_ADDRESS")
	os.Unsetenv("KAFKA_TOPIC")
	cfg := Load()
	if cfg.Kafka.Topic != "test-topic" {
		t.Fatalf("Topic = %q, want default %q", cfg.Kafka.Topic, "test-topic")
	}
}
