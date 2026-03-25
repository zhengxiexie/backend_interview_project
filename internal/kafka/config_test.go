package kafka

import (
	"strings"
	"testing"
	"time"
)

func validTestConfig() Config {
	return Config{
		BrokerAddress:    "localhost:9092",
		Topic:            "test-topic",
		GroupID:          "test-group",
		Partitions:       3,
		WorkerCount:      2,
		QueueCapacity:    32,
		MaxRetryAttempts: 3,
		RetryBaseDelay:   100 * time.Millisecond,
		RetryMaxDelay:    time.Second,
		CommitTimeout:    5 * time.Second,
		DrainTimeout:     5 * time.Second,
	}
}

func TestConfigValidateAcceptsValidConfig(t *testing.T) {
	cfg := validTestConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error = %v", err)
	}
}

func TestConfigValidateRejectsEmptyBrokerAddress(t *testing.T) {
	cfg := validTestConfig()
	cfg.BrokerAddress = ""
	assertValidationError(t, cfg, "BrokerAddress")
}

func TestConfigValidateRejectsEmptyTopic(t *testing.T) {
	cfg := validTestConfig()
	cfg.Topic = ""
	assertValidationError(t, cfg, "Topic")
}

func TestConfigValidateRejectsEmptyGroupID(t *testing.T) {
	cfg := validTestConfig()
	cfg.GroupID = ""
	assertValidationError(t, cfg, "GroupID")
}

func TestConfigValidateRejectsZeroPartitions(t *testing.T) {
	cfg := validTestConfig()
	cfg.Partitions = 0
	assertValidationError(t, cfg, "Partitions")
}

func TestConfigValidateRejectsZeroWorkerCount(t *testing.T) {
	cfg := validTestConfig()
	cfg.WorkerCount = 0
	assertValidationError(t, cfg, "WorkerCount")
}

func TestConfigValidateRejectsNegativeRetryBaseDelay(t *testing.T) {
	cfg := validTestConfig()
	cfg.RetryBaseDelay = -time.Second
	assertValidationError(t, cfg, "RetryBaseDelay")
}

func TestConfigValidateRejectsRetryMaxDelayBelowBaseDelay(t *testing.T) {
	cfg := validTestConfig()
	cfg.RetryBaseDelay = time.Second
	cfg.RetryMaxDelay = time.Millisecond
	assertValidationError(t, cfg, "RetryMaxDelay")
}

func TestConfigValidateRejectsNegativeCommitTimeout(t *testing.T) {
	cfg := validTestConfig()
	cfg.CommitTimeout = -time.Second
	assertValidationError(t, cfg, "CommitTimeout")
}

func TestConfigValidateRejectsNegativeDrainTimeout(t *testing.T) {
	cfg := validTestConfig()
	cfg.DrainTimeout = -time.Second
	assertValidationError(t, cfg, "DrainTimeout")
}

func TestConfigValidateCollectsMultipleErrors(t *testing.T) {
	cfg := Config{} // everything zero/empty
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for empty config")
	}
	msg := err.Error()
	for _, want := range []string{"BrokerAddress", "Topic", "GroupID", "Partitions", "WorkerCount"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Validate() error = %q, want mention of %q", msg, want)
		}
	}
}

func assertValidationError(t *testing.T, cfg Config, field string) {
	t.Helper()
	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate() expected error for invalid %s", field)
	}
	if !strings.Contains(err.Error(), field) {
		t.Fatalf("Validate() error = %q, want mention of %q", err.Error(), field)
	}
}
