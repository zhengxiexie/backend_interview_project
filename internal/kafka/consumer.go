package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type messageReader interface {
	messageCommitter
	FetchMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// Consumer owns the runnable consumption pipeline for one consumer-group member.
//
// The critical design choice is that fetch, execution, partition-local correctness,
// and final commit are kept separate:
//   - the shared reader controls intake from Kafka
//   - the shared worker pool maximizes concurrency
//   - partitionState keeps correctness decisions partition-local
//   - commits happen only after a contiguous frontier advances
//
// This separation is what lets the implementation support same-partition
// concurrency without allowing offset skipping.
type Consumer struct {
	cfg            Config
	reader         messageReader
	handlerFactory HandlerFactory
	retryPolicy    RetryPolicy
	commitMu       sync.Mutex
	partitionsMu   sync.Mutex
	partitions     map[int]*partitionState
	activeJobs     atomic.Int64
	processedCount atomic.Int64
}

// NewConsumer constructs a Consumer wired to a real Kafka broker via a
// consumer-group reader. For testing, prefer newConsumer with injected doubles.
func NewConsumer(cfg Config) *Consumer {
	return newConsumer(
		cfg,
		newKafkaReader(cfg),
		NewHandlerFactory(cfg),
		NewRetryPolicy(cfg.MaxRetryAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay),
	)
}

func newConsumer(cfg Config, reader messageReader, handlerFactory HandlerFactory, retryPolicy RetryPolicy) *Consumer {
	if reader == nil {
		reader = newKafkaReader(cfg)
	}
	if handlerFactory == nil {
		handlerFactory = NewHandlerFactory(cfg)
	}
	// A zero-value RetryPolicy means the caller did not configure one explicitly;
	// fall back to cfg so tests and wrappers can omit it without accidentally
	// disabling all retries.
	if retryPolicy == (RetryPolicy{}) {
		retryPolicy = NewRetryPolicy(cfg.MaxRetryAttempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	}

	return &Consumer{
		cfg:            cfg,
		reader:         reader,
		handlerFactory: handlerFactory,
		retryPolicy:    NewRetryPolicy(retryPolicy.MaxAttempts, retryPolicy.BaseDelay, retryPolicy.MaxDelay),
		partitions:     make(map[int]*partitionState),
	}
}

func newKafkaReader(cfg Config) *kafka.Reader {
	// GroupID keeps offset persistence inside Kafka's consumer-group model while
	// FetchMessage/CommitMessages still lets this project make commit timing
	// explicit in code.
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BrokerAddress},
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxAttempts:    10,
		ReadBackoffMin: cfg.RetryBaseDelay,
		ReadBackoffMax: cfg.RetryMaxDelay,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,
	})
}

// ConsumeMessages runs the consumer pipeline until ctx is cancelled or a fatal
// error occurs. It is not safe to call ConsumeMessages more than once on the
// same Consumer; the partition state accumulated during a run is not reset
// between calls. Construct a new Consumer for each independent run.
func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	log.Printf("Starting consumer group %q for topic %q", c.cfg.GroupID, c.cfg.Topic)

	// jobs is the only hand-off point between intake and execution. Once a
	// message is fetched and tracked, it must either finish successfully or stay
	// uncommitted for replay; there is no best-effort drop path.
	jobs := make(chan messageJob, c.cfg.QueueCapacity)
	errCh := make(chan error, 1)
	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()

	// workerCtx is derived from context.Background() rather than ctx so that
	// workers can continue processing in-flight jobs after ctx is cancelled
	// for graceful shutdown. Workers are stopped explicitly by cancelWorkers()
	// only after the drain window has closed.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	var workersWG sync.WaitGroup
	c.startWorkers(&workersWG, workerCtx, jobs, errCh, stopIntake)

	fetchErr := c.fetchLoop(intakeCtx, jobs)
	c.drainAfterIntakeStops(fetchErr)

	cancelWorkers()
	close(jobs)
	workersWG.Wait()

	return c.combineRunErrors(fetchErr, errCh)
}

func (c *Consumer) fetchLoop(ctx context.Context, jobs chan<- messageJob) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Consumer fetch loop stopping because context was cancelled")
				return ctx.Err()
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		state := c.partitionState(msg)
		// Tracking happens before the job is queued so shutdown or worker crashes
		// cannot lose visibility of already-fetched offsets.
		//
		// If ctx.Done() wins the select below, this message is tracked (MarkInFlight)
		// but never dispatched to a worker. Its offset stays uncommitted and will be
		// replayed after restart or rebalance — the at-least-once safety net.
		state.trackFetched(msg)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- messageJob{msg: msg}:
			c.activeJobs.Add(1)
		}
	}
}

func (c *Consumer) startWorkers(workersWG *sync.WaitGroup, workerCtx context.Context, jobs <-chan messageJob, errCh chan<- error, stopIntake context.CancelFunc) {
	workerCount := c.cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		workersWG.Add(1)
		go func(workerID int) {
			defer workersWG.Done()
			c.workerLoop(workerCtx, workerID, jobs, errCh, stopIntake)
		}(i + 1)
	}
}

func (c *Consumer) workerLoop(ctx context.Context, workerID int, jobs <-chan messageJob, errCh chan<- error, stopIntake context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			if err := c.handleJob(ctx, workerID, job); err != nil {
				c.reportWorkerError(errCh, stopIntake, err)
			}
			c.activeJobs.Add(-1)
		}
	}
}

func (c *Consumer) handleJob(ctx context.Context, workerID int, job messageJob) error {
	state := c.partitionState(job.msg)

	for attempt := 1; ; attempt++ {
		// Returning nil on context cancellation is intentional: the message's
		// offset stays uncommitted (tracked as in-flight by OffsetTracker) and
		// will be replayed on restart or rebalance — preserving the at-least-once
		// guarantee without treating shutdown as an error.
		if err := ctx.Err(); err != nil {
			return nil
		}

		log.Printf("Worker %d processing partition %d offset %d attempt %d", workerID, job.msg.Partition, job.msg.Offset, attempt)
		err := state.handle(ctx, job.msg)
		if err == nil {
			// CommitProcessed is the only place allowed to turn a local success into
			// a Kafka commit. Workers never commit directly.
			if commitErr := state.commitProcessed(c.reader, job.msg); commitErr != nil {
				return commitErr
			}
			c.processedCount.Add(1)
			return nil
		}
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}

		decision := c.retryPolicy.Decide(attempt)
		if !decision.Retry {
			log.Printf("Message partition %d offset %d exhausted retries (%d attempts); leaving offset uncommitted for replay on restart/rebalance",
				job.msg.Partition, job.msg.Offset, attempt)
			return nil
		}

		log.Printf("Message partition %d offset %d failed on attempt %d: %v; retrying in %s", job.msg.Partition, job.msg.Offset, attempt, err, decision.Delay)
		retryTimer := time.NewTimer(decision.Delay)
		select {
		case <-ctx.Done():
			retryTimer.Stop()
			return nil
		case <-retryTimer.C:
		}
	}
}

func (c *Consumer) partitionState(msg kafka.Message) *partitionState {
	c.partitionsMu.Lock()
	defer c.partitionsMu.Unlock()

	state, ok := c.partitions[msg.Partition]
	if ok {
		return state
	}

	// lastCommitted starts one behind the first seen offset for this partition.
	// That keeps the frontier logic simple: the next safe candidate is always
	// lastCommitted + 1.
	lastCommitted := msg.Offset - 1
	state = newPartitionState(c.cfg, msg.Partition, lastCommitted, &c.commitMu, c.handlerFactory())
	c.partitions[msg.Partition] = state
	return state
}

func (c *Consumer) reportWorkerError(errCh chan<- error, stopIntake context.CancelFunc, err error) {
	// A fatal worker-side error means the process can no longer make trustworthy
	// commit decisions. Intake is stopped first, then the pipeline drains what is
	// already in flight.
	stopIntake()
	select {
	case errCh <- err:
	default:
	}
}

func (c *Consumer) drainAfterIntakeStops(fetchErr error) {
	// fetchLoop only exits with a non-nil error (context cancellation or a real
	// fetch error); it never returns nil. This guard is defensive.
	if fetchErr == nil {
		return
	}
	if c.cfg.DrainTimeout <= 0 {
		return
	}

	if errors.Is(fetchErr, context.Canceled) {
		log.Printf("Consumer intake stopped; draining in-flight work for up to %s", c.cfg.DrainTimeout)
	} else {
		log.Printf("Consumer intake failed (%v); draining in-flight work for up to %s", fetchErr, c.cfg.DrainTimeout)
	}

	c.waitForDrain(c.cfg.DrainTimeout)
}

func (c *Consumer) combineRunErrors(fetchErr error, errCh <-chan error) error {
	select {
	case workerErr := <-errCh:
		// If fetch stopped cleanly (nil or context cancellation), the worker
		// error is the only genuine failure to surface.
		if IgnoreContextCancellation(fetchErr) == nil {
			return workerErr
		}
		return errors.Join(fetchErr, workerErr)
	default:
	}

	return IgnoreContextCancellation(fetchErr)
}

func (c *Consumer) waitForDrain(timeout time.Duration) {
	if timeout <= 0 {
		return
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(c.quiescenceWindow())
	defer ticker.Stop()

	for {
		remaining := c.activeJobs.Load()
		if remaining == 0 {
			log.Println("All in-flight consumer work drained")
			return
		}

		select {
		case <-deadline.C:
			log.Printf("Drain timeout reached with %d in-flight jobs still active", c.activeJobs.Load())
			return
		case <-ticker.C:
			// next poll
		}
	}
}

func (c *Consumer) quiescenceWindow() time.Duration {
	if c.cfg.QuiescenceWindow > 0 {
		return c.cfg.QuiescenceWindow
	}
	return 100 * time.Millisecond
}

// Close releases the underlying Kafka reader. It must be called only after
// ConsumeMessages has returned; calling it concurrently with ConsumeMessages
// may cause undefined behavior in the reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// ProcessedCount returns the number of messages successfully processed and
// committed since this Consumer was constructed. It is safe to call
// concurrently with ConsumeMessages.
func (c *Consumer) ProcessedCount() int64 {
	return c.processedCount.Load()
}
