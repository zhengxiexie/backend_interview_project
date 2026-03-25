package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/kafka"
)

// fakeKafkaManager is a test double for runtime bootstrap and teardown.
type fakeKafkaManager struct {
	startErr error
	started  bool
	closed   bool
}

func (m *fakeKafkaManager) Start(context.Context) error {
	if m.startErr != nil {
		return m.startErr
	}
	m.started = true
	return nil
}

func (m *fakeKafkaManager) Close() error {
	m.closed = true
	return nil
}

// fakeProducer lets runtime tests drive success and failure paths deterministically.
type fakeProducer struct {
	err    error
	closed bool
}

func (p *fakeProducer) ProduceMessages(context.Context, int) error {
	return p.err
}

func (p *fakeProducer) Close() error {
	p.closed = true
	return nil
}

// fakeConsumer exposes controlled consume behavior and processed counts for
// catch-up and error-path runtime tests.
type fakeConsumer struct {
	mu        sync.Mutex
	processed int64
	consume   func(ctx context.Context) error
	closed    bool
}

func (c *fakeConsumer) ConsumeMessages(ctx context.Context) error {
	if c.consume != nil {
		return c.consume(ctx)
	}
	<-ctx.Done()
	return nil
}

func (c *fakeConsumer) ProcessedCount() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processed
}

func (c *fakeConsumer) setProcessedCount(v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.processed = v
}

func (c *fakeConsumer) Close() error {
	c.closed = true
	return nil
}

func testRuntimeConfig() *config.Config {
	return &config.Config{
		Kafka: kafka.Config{
			BrokerAddress:    "localhost:29092",
			Topic:            "test-topic",
			GroupID:          "test-group",
			Partitions:       1,
			WorkerCount:      1,
			DrainTimeout:     200 * time.Millisecond,
			QuiescenceWindow: 10 * time.Millisecond,
		},
		App: config.AppConfig{NumMessages: 3},
	}
}

func TestRuntimeRunReturnsProducerError(t *testing.T) {
	manager := &fakeKafkaManager{}
	fakeProd := &fakeProducer{err: errors.New("producer boom")}
	fakeCons := &fakeConsumer{
		consume: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
	}

	runtime := NewRuntime(testRuntimeConfig())
	runtime.deps = runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) { return manager, nil },
		recreateTopic:   func(context.Context, kafka.Config) error { return nil },
		newProducer:     func(kafka.Config) producer { return fakeProd },
		newConsumer:     func(kafka.Config) consumer { return fakeCons },
	}

	err := runtime.Run("producer-error")
	if err == nil || !errors.Is(err, fakeProd.err) {
		t.Fatalf("Run() error = %v, want producer error %v", err, fakeProd.err)
	}
	if !manager.started || !manager.closed {
		t.Fatalf("expected kafka manager to start and close, got started=%t closed=%t", manager.started, manager.closed)
	}
	if !fakeProd.closed {
		t.Fatal("expected producer to be closed")
	}
	if !fakeCons.closed {
		t.Fatal("expected consumer to be closed")
	}
}

func TestRuntimeRunReturnsConsumerError(t *testing.T) {
	manager := &fakeKafkaManager{}
	consumerErr := errors.New("consumer boom")
	fakeProd := &fakeProducer{}
	fakeCons := &fakeConsumer{
		consume: func(ctx context.Context) error {
			return consumerErr
		},
	}

	runtime := NewRuntime(testRuntimeConfig())
	runtime.deps = runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) { return manager, nil },
		recreateTopic:   func(context.Context, kafka.Config) error { return nil },
		newProducer:     func(kafka.Config) producer { return fakeProd },
		newConsumer:     func(kafka.Config) consumer { return fakeCons },
	}

	err := runtime.Run("consumer-error")
	if err == nil || !errors.Is(err, consumerErr) {
		t.Fatalf("Run() error = %v, want consumer error %v", err, consumerErr)
	}
	if !manager.started || !manager.closed {
		t.Fatalf("expected kafka manager to start and close, got started=%t closed=%t", manager.started, manager.closed)
	}
	if !fakeProd.closed {
		t.Fatal("expected producer to be closed")
	}
	if !fakeCons.closed {
		t.Fatal("expected consumer to be closed")
	}
}

func TestRuntimeBootstrapFailureReturnsError(t *testing.T) {
	startErr := errors.New("docker unavailable")
	manager := &fakeKafkaManager{startErr: startErr}

	runtime := NewRuntime(testRuntimeConfig())
	runtime.deps = runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) { return manager, nil },
		recreateTopic:   func(context.Context, kafka.Config) error { return nil },
		newProducer:     func(kafka.Config) producer { return &fakeProducer{} },
		newConsumer:     func(kafka.Config) consumer { return &fakeConsumer{} },
	}

	err := runtime.Run("bootstrap-failure")
	if err == nil || !errors.Is(err, startErr) {
		t.Fatalf("Run() error = %v, want start error %v", err, startErr)
	}
	// bootstrap calls Close() on the manager when Start fails.
	if !manager.closed {
		t.Fatal("expected kafka manager to be closed after start failure")
	}
}

func TestRuntimeRunWaitsForCatchUpAfterProducerCompletes(t *testing.T) {
	manager := &fakeKafkaManager{}
	fakeProd := &fakeProducer{}
	fakeCons := &fakeConsumer{}
	fakeCons.setProcessedCount(3)
	fakeCons.consume = func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}

	runtime := NewRuntime(testRuntimeConfig())
	runtime.deps = runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) { return manager, nil },
		recreateTopic:   func(context.Context, kafka.Config) error { return nil },
		newProducer:     func(kafka.Config) producer { return fakeProd },
		newConsumer:     func(kafka.Config) consumer { return fakeCons },
	}

	if err := runtime.Run("producer-success"); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if !manager.started || !manager.closed {
		t.Fatalf("expected kafka manager to start and close, got started=%t closed=%t", manager.started, manager.closed)
	}
	if !fakeProd.closed {
		t.Fatal("expected producer to be closed")
	}
	if !fakeCons.closed {
		t.Fatal("expected consumer to be closed")
	}
}

func TestRuntimeRunRejectsInvalidConfig(t *testing.T) {
	cfg := testRuntimeConfig()
	cfg.Kafka.BrokerAddress = "" // invalid

	runtime := NewRuntime(cfg)
	runtime.deps = runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) { return &fakeKafkaManager{}, nil },
		recreateTopic:   func(context.Context, kafka.Config) error { return nil },
		newProducer:     func(kafka.Config) producer { return &fakeProducer{} },
		newConsumer:     func(kafka.Config) consumer { return &fakeConsumer{} },
	}

	err := runtime.Run("invalid-config")
	if err == nil {
		t.Fatal("Run() expected error for invalid config, got nil")
	}
}
