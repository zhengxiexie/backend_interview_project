package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

// kafkaManager abstracts the Docker-backed Kafka lifecycle so runtime tests can
// swap in a fake implementation.
type kafkaManager interface {
	Start(ctx context.Context) error
	Close() error
}

// producer abstracts message production for runtime orchestration tests.
type producer interface {
	ProduceMessages(ctx context.Context, numMessages int) error
	Close() error
}

// consumer abstracts the runnable consumer pipeline for runtime orchestration
// tests and catch-up coordination.
type consumer interface {
	ConsumeMessages(ctx context.Context) error
	ProcessedCount() int64
	Close() error
}

// runtimeDependencies gathers constructor hooks so Runtime can be tested without
// booting Docker or Kafka.
type runtimeDependencies struct {
	newKafkaManager func() (kafkaManager, error)
	recreateTopic   func(ctx context.Context, cfg kafka.Config) error
	newProducer     func(cfg kafka.Config) producer
	newConsumer     func(cfg kafka.Config) consumer
}

// runtimeResources groups the live objects that must be shut down together in
// reverse order of use.
type runtimeResources struct {
	kafkaManager kafkaManager
	producer     producer
	consumer     consumer
}

// Runtime coordinates the demo application's full lifecycle: bootstrap Kafka,
// start producer/consumer, wait for catch-up, and shut everything down
// conservatively.
type Runtime struct {
	Config *config.Config
	deps   runtimeDependencies
}

// NewRuntime constructs a Runtime wired to real Docker and Kafka
// dependencies. Pass a nil cfg to load defaults from config.Load().
func NewRuntime(cfg *config.Config) Runtime {
	return Runtime{
		Config: cfg,
		deps:   defaultRuntimeDependencies(),
	}
}

func defaultRuntimeDependencies() runtimeDependencies {
	return runtimeDependencies{
		newKafkaManager: func() (kafkaManager, error) {
			return docker.NewKafkaManager()
		},
		recreateTopic: kafka.RecreateTopic,
		newProducer: func(cfg kafka.Config) producer {
			return kafka.NewProducer(cfg)
		},
		newConsumer: func(cfg kafka.Config) consumer {
			return kafka.NewConsumer(cfg)
		},
	}
}

func (r Runtime) Run(runLabel string) error {
	cfg := r.runtimeConfig()
	deps := r.dependencies()

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	log.Printf("Starting Kafka producer/consumer application (%s)...", runLabel)

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	resources, err := r.bootstrap(signalCtx, cfg, deps)
	if err != nil {
		return err
	}
	defer resources.close()

	return r.runPipeline(signalCtx, cfg, resources)
}

func (r Runtime) runtimeConfig() *config.Config {
	if r.Config != nil {
		return r.Config
	}
	return config.Load()
}

func (r Runtime) dependencies() runtimeDependencies {
	deps := r.deps
	defaults := defaultRuntimeDependencies()

	if deps.newKafkaManager == nil {
		deps.newKafkaManager = defaults.newKafkaManager
	}
	if deps.recreateTopic == nil {
		deps.recreateTopic = defaults.recreateTopic
	}
	if deps.newProducer == nil {
		deps.newProducer = defaults.newProducer
	}
	if deps.newConsumer == nil {
		deps.newConsumer = defaults.newConsumer
	}

	return deps
}

func (r Runtime) bootstrap(ctx context.Context, cfg *config.Config, deps runtimeDependencies) (runtimeResources, error) {
	log.Println("Starting Kafka container programmatically...")
	manager, err := deps.newKafkaManager()
	if err != nil {
		return runtimeResources{}, fmt.Errorf("create Kafka manager: %w", err)
	}

	if err := manager.Start(ctx); err != nil {
		if closeErr := manager.Close(); closeErr != nil {
			log.Printf("Warning: failed to close Kafka manager after start failure: %v", closeErr)
		}
		return runtimeResources{}, fmt.Errorf("start Kafka: %w", err)
	}

	if err := deps.recreateTopic(ctx, cfg.Kafka); err != nil {
		if closeErr := manager.Close(); closeErr != nil {
			log.Printf("Warning: failed to close Kafka manager after topic setup failure: %v", closeErr)
		}
		return runtimeResources{}, fmt.Errorf("recreate topic with %d partitions: %w", cfg.Kafka.Partitions, err)
	}

	return runtimeResources{
		kafkaManager: manager,
		producer:     deps.newProducer(cfg.Kafka),
		consumer:     deps.newConsumer(cfg.Kafka),
	}, nil
}

func (r Runtime) runPipeline(signalCtx context.Context, cfg *config.Config, resources runtimeResources) error {
	runCtx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	producerDone := make(chan error, 1)
	consumerDone := make(chan error, 1)

	go func() {
		log.Println("Starting consumer...")
		consumerDone <- resources.consumer.ConsumeMessages(runCtx)
	}()

	go func() {
		log.Println("Starting producer...")
		producerDone <- resources.producer.ProduceMessages(runCtx, cfg.App.NumMessages)
	}()

	var producerErr error
	var consumerErr error
	producerReceived := false
	consumerReceived := false

	select {
	case <-signalCtx.Done():
		log.Println("Received interrupt signal, beginning graceful shutdown...")
		cancel()
	case producerErr = <-producerDone:
		producerReceived = true
		if producerErr != nil {
			log.Printf("Producer error: %v", producerErr)
			cancel()
		} else {
			// The producer finishing is not enough to stop safely. The consumer gets
			// a bounded catch-up window so already-written messages still have a
			// chance to finish and commit before intake is canceled.
			log.Println("Producer finished sending messages; waiting for consumer to catch up before shutdown...")
			waitForConsumerCatchUp(signalCtx, resources.consumer, int64(cfg.App.NumMessages), cfg.Kafka.DrainTimeout, cfg.Kafka.QuiescenceWindow)
			cancel()
		}
	case consumerErr = <-consumerDone:
		consumerReceived = true
		if consumerErr == nil {
			consumerErr = errors.New("consumer exited before shutdown coordination completed")
		}
		if !errors.Is(consumerErr, context.Canceled) {
			log.Printf("Consumer error: %v", consumerErr)
		}
		cancel()
	}

	if !producerReceived {
		producerErr = <-producerDone
	}
	if !consumerReceived {
		consumerErr = <-consumerDone
	}

	log.Printf("Consumer stopped after processing %d messages", resources.consumer.ProcessedCount())
	return joinComponentErrors(producerErr, consumerErr)
}

func (r runtimeResources) close() {
	log.Println("Closing producer and consumer...")
	if r.producer != nil {
		if err := r.producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}
	if r.consumer != nil {
		if err := r.consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}

	if r.kafkaManager != nil {
		log.Println("Closing Kafka runtime...")
		if err := r.kafkaManager.Close(); err != nil {
			log.Printf("Error closing Kafka manager: %v", err)
		}
	}
	log.Println("Application shutdown complete")
}

func joinComponentErrors(producerErr, consumerErr error) error {
	producerErr = kafka.IgnoreContextCancellation(producerErr)
	consumerErr = kafka.IgnoreContextCancellation(consumerErr)

	switch {
	case producerErr != nil && consumerErr != nil:
		return errors.Join(fmt.Errorf("producer: %w", producerErr), fmt.Errorf("consumer: %w", consumerErr))
	case producerErr != nil:
		return fmt.Errorf("producer: %w", producerErr)
	case consumerErr != nil:
		return fmt.Errorf("consumer: %w", consumerErr)
	default:
		return nil
	}
}

// waitForConsumerCatchUp polls the consumer's ProcessedCount until it reaches
// expectedCount or the timeout expires, whichever comes first. If the parent
// context is cancelled (e.g. by SIGINT during the catch-up window), polling
// stops immediately so the process can proceed with shutdown.
//
// expectedCount is intentionally used as a best-effort target: when every
// produced message is also processed, this condition terminates the catch-up
// window early. If messages are lost or duplicated upstream, the timeout
// provides the fallback boundary — the consumer's at-least-once guarantee
// handles the remainder on restart.
func waitForConsumerCatchUp(ctx context.Context, c consumer, expectedCount int64, timeout time.Duration, pollInterval time.Duration) {
	if timeout <= 0 {
		return
	}
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		processed := c.ProcessedCount()
		if processed >= expectedCount {
			log.Printf("Consumer caught up: processed %d/%d messages", processed, expectedCount)
			return
		}

		select {
		case <-ctx.Done():
			log.Printf("Consumer catch-up interrupted by signal: processed %d/%d messages", c.ProcessedCount(), expectedCount)
			return
		case <-deadline.C:
			log.Printf("Consumer catch-up timeout: processed %d/%d messages", c.ProcessedCount(), expectedCount)
			return
		case <-ticker.C:
			// next poll iteration
		}
	}
}
