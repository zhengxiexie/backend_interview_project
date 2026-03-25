package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

func TestSimulatedMessageHandlerSucceedsWithNoConfig(t *testing.T) {
	h := NewSimulatedMessageHandler(Config{})
	msg := segmentio.Message{Key: []byte("key-0"), Value: []byte("v")}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}

func TestSimulatedMessageHandlerRespectsGlobalProcessingDelay(t *testing.T) {
	delay := 50 * time.Millisecond
	h := NewSimulatedMessageHandler(Config{ProcessingDelay: delay})
	msg := segmentio.Message{Key: []byte("key-0")}

	start := time.Now()
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("Handle() returned in %v, want >= %v", elapsed, delay)
	}
}

func TestSimulatedMessageHandlerPerKeyDelayOverridesGlobal(t *testing.T) {
	globalDelay := 500 * time.Millisecond
	keyDelay := 10 * time.Millisecond
	h := NewSimulatedMessageHandler(Config{
		ProcessingDelay: globalDelay,
		PerKeyProcessingDelays: map[string]time.Duration{
			"key-fast": keyDelay,
		},
	})

	msg := segmentio.Message{Key: []byte("key-fast")}
	start := time.Now()
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed >= globalDelay {
		t.Fatalf("per-key delay not applied: elapsed %v >= global delay %v", elapsed, globalDelay)
	}
}

func TestSimulatedMessageHandlerInjectsFailureForConfiguredKey(t *testing.T) {
	h := NewSimulatedMessageHandler(Config{
		Simulation: SimulationConfig{
			FailureKeys:     []string{"key-bad"},
			FailureAttempts: 2,
		},
	})
	msg := segmentio.Message{Key: []byte("key-bad")}

	// First two calls must fail.
	for i := 1; i <= 2; i++ {
		if err := h.Handle(context.Background(), msg); err == nil {
			t.Fatalf("attempt %d: Handle() returned nil, want failure", i)
		}
	}

	// Third call must succeed.
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatalf("attempt 3: Handle() error = %v, want nil", err)
	}
}

func TestSimulatedMessageHandlerDoesNotFailUnlistedKeys(t *testing.T) {
	h := NewSimulatedMessageHandler(Config{
		Simulation: SimulationConfig{
			FailureKeys:     []string{"key-bad"},
			FailureAttempts: 5,
		},
	})
	msg := segmentio.Message{Key: []byte("key-good")}

	for i := 0; i < 3; i++ {
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatalf("Handle() on unlisted key error = %v, want nil", err)
		}
	}
}

func TestSimulatedMessageHandlerContextCancellationDuringDelay(t *testing.T) {
	h := NewSimulatedMessageHandler(Config{ProcessingDelay: 10 * time.Second})
	msg := segmentio.Message{Key: []byte("key-0")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before Handle is called

	err := h.Handle(ctx, msg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle() error = %v, want context.Canceled", err)
	}
}
