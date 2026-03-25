package kafka

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

// fakeMessageReader is a test double that combines fetch and commit behavior so
// consumer tests can exercise the full pipeline without a real Kafka broker.
type fakeMessageReader struct {
	mu             sync.Mutex
	messages       []segmentio.Message
	committed      []int64
	commitErr      error
	fetchCallCount int
}

func (r *fakeMessageReader) FetchMessage(ctx context.Context) (segmentio.Message, error) {
	r.mu.Lock()
	r.fetchCallCount++
	if len(r.messages) > 0 {
		msg := r.messages[0]
		r.messages = r.messages[1:]
		r.mu.Unlock()
		return msg, nil
	}
	r.mu.Unlock()

	<-ctx.Done()
	return segmentio.Message{}, ctx.Err()
}

func (r *fakeMessageReader) CommitMessages(_ context.Context, msgs ...segmentio.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, msg := range msgs {
		r.committed = append(r.committed, msg.Offset)
	}
	return nil
}

func (r *fakeMessageReader) Close() error { return nil }

func (r *fakeMessageReader) committedOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.committed...)
}

// blockingHandler lets a test prove that shutdown waits for already-started work
// instead of returning immediately.
type blockingHandler struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *blockingHandler) Handle(ctx context.Context, _ segmentio.Message) error {
	h.once.Do(func() { close(h.started) })
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// successfulHandler is the simplest possible success-only handler used in tests
// that focus on commit and shutdown control flow.
type successfulHandler struct{}

func (successfulHandler) Handle(context.Context, segmentio.Message) error { return nil }

func TestConsumerDrainsInFlightWorkAfterCancellation(t *testing.T) {
	reader := &fakeMessageReader{
		messages: []segmentio.Message{{Partition: 0, Offset: 0, Key: []byte("key-0"), Value: []byte("value-0")}},
	}
	handler := &blockingHandler{started: make(chan struct{}), release: make(chan struct{})}
	consumer := newConsumer(
		Config{WorkerCount: 1, QueueCapacity: 1, DrainTimeout: time.Second, QuiescenceWindow: 10 * time.Millisecond},
		reader,
		func() MessageHandler { return handler },
		NewRetryPolicy(1, 10*time.Millisecond, 10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.ConsumeMessages(ctx)
	}()

	<-handler.started
	cancel()

	select {
	case err := <-done:
		t.Fatalf("ConsumeMessages() returned before in-flight work drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(handler.release)

	if err := <-done; err != nil {
		t.Fatalf("ConsumeMessages() error = %v, want nil", err)
	}
	if got := consumer.ProcessedCount(); got != 1 {
		t.Fatalf("ProcessedCount() = %d, want 1", got)
	}
	if got := reader.committedOffsets(); !reflect.DeepEqual(got, []int64{0}) {
		t.Fatalf("committed offsets = %v, want [0]", got)
	}
}

// countingHandler tracks invocation count so retry tests can verify how many
// attempts occurred, and can be configured to fail the first N calls.
type countingHandler struct {
	mu        sync.Mutex
	calls     int
	failUntil int // fail on the first failUntil calls (calls <= failUntil returns error)
}

func (h *countingHandler) Handle(_ context.Context, _ segmentio.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.calls <= h.failUntil {
		return errors.New("transient failure")
	}
	return nil
}

func (h *countingHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// cancelWhenReaderBlocks waits until fakeMessageReader has exhausted its
// pre-loaded messages and is blocking on ctx.Done(), then cancels the context.
// This lets retry tests shut down cleanly without relying on a timeout.
func cancelWhenReaderBlocks(t *testing.T, reader *fakeMessageReader, cancel context.CancelFunc) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reader.mu.Lock()
		count := reader.fetchCallCount
		reader.mu.Unlock()
		if count >= 2 { // first call returns the message; second call blocks
			cancel()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for reader to reach blocking state")
}

func TestConsumerRetrySucceedsOnSecondAttempt(t *testing.T) {
	reader := &fakeMessageReader{
		messages: []segmentio.Message{{Partition: 0, Offset: 0, Key: []byte("key-0"), Value: []byte("v")}},
	}
	handler := &countingHandler{failUntil: 1} // fail attempt 1, succeed attempt 2
	consumer := newConsumer(
		Config{WorkerCount: 1, QueueCapacity: 1, DrainTimeout: time.Second, QuiescenceWindow: 10 * time.Millisecond},
		reader,
		func() MessageHandler { return handler },
		NewRetryPolicy(3, time.Millisecond, 10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.ConsumeMessages(ctx) }()

	cancelWhenReaderBlocks(t, reader, cancel)

	if err := <-done; err != nil {
		t.Fatalf("ConsumeMessages() error = %v, want nil", err)
	}
	if got := consumer.ProcessedCount(); got != 1 {
		t.Fatalf("ProcessedCount() = %d, want 1", got)
	}
	if got := reader.committedOffsets(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("committed offsets = %v, want [0]", got)
	}
	if got := handler.callCount(); got != 2 {
		t.Fatalf("handler called %d times, want 2", got)
	}
}

func TestConsumerRetryExhaustedLeavesOffsetUncommitted(t *testing.T) {
	reader := &fakeMessageReader{
		messages: []segmentio.Message{{Partition: 0, Offset: 0, Key: []byte("key-0"), Value: []byte("v")}},
	}
	handler := &countingHandler{failUntil: 99} // always fail
	consumer := newConsumer(
		Config{WorkerCount: 1, QueueCapacity: 1, DrainTimeout: time.Second, QuiescenceWindow: 10 * time.Millisecond},
		reader,
		func() MessageHandler { return handler },
		// MaxAttempts=2 allows 2 retries → 3 total processing attempts before exhaustion.
		NewRetryPolicy(2, time.Millisecond, 10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.ConsumeMessages(ctx) }()

	cancelWhenReaderBlocks(t, reader, cancel)

	if err := <-done; err != nil {
		t.Fatalf("ConsumeMessages() error = %v, want nil", err)
	}
	if got := consumer.ProcessedCount(); got != 0 {
		t.Fatalf("ProcessedCount() = %d, want 0", got)
	}
	if got := reader.committedOffsets(); len(got) != 0 {
		t.Fatalf("committed offsets = %v, want none", got)
	}
	// 1 original attempt + 2 retries = 3 handler calls before exhaustion.
	if got := handler.callCount(); got != 3 {
		t.Fatalf("handler called %d times, want 3 (1 original + 2 retries)", got)
	}
}

func TestConsumerProcessesMessagesAcrossMultiplePartitions(t *testing.T) {
	// Three messages, one per partition. Each partition gets its own
	// partitionState and must commit independently.
	reader := &fakeMessageReader{
		messages: []segmentio.Message{
			{Partition: 0, Offset: 0, Key: []byte("key-0"), Value: []byte("v0")},
			{Partition: 1, Offset: 0, Key: []byte("key-1"), Value: []byte("v1")},
			{Partition: 2, Offset: 0, Key: []byte("key-2"), Value: []byte("v2")},
		},
	}
	consumer := newConsumer(
		Config{WorkerCount: 3, QueueCapacity: 3, DrainTimeout: time.Second, QuiescenceWindow: 10 * time.Millisecond},
		reader,
		func() MessageHandler { return successfulHandler{} },
		NewRetryPolicy(1, time.Millisecond, 10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.ConsumeMessages(ctx) }()

	// Wait until all 3 messages have been processed, then shut down.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if consumer.ProcessedCount() == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if consumer.ProcessedCount() != 3 {
		cancel()
		t.Fatalf("timed out: ProcessedCount() = %d, want 3 after 5s", consumer.ProcessedCount())
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("ConsumeMessages() error = %v, want nil", err)
	}
	if got := consumer.ProcessedCount(); got != 3 {
		t.Fatalf("ProcessedCount() = %d, want 3", got)
	}
	// Each partition commits its own offset 0 independently: expect 3 commits.
	if got := reader.committedOffsets(); len(got) != 3 {
		t.Fatalf("committed offset count = %d, want 3 (one per partition): %v", len(got), got)
	}
}

func TestCombineRunErrorsJoinsFetchAndWorkerErrors(t *testing.T) {
	// When the fetch loop fails with a real error AND a worker also failed,
	// both errors must be joined so neither is silently dropped.
	fetchErr := errors.New("broker connection lost")
	workerErr := errors.New("commit failed")

	errCh := make(chan error, 1)
	errCh <- workerErr

	c := &Consumer{}
	err := c.combineRunErrors(fetchErr, errCh)

	if err == nil {
		t.Fatal("combineRunErrors() = nil, want combined error")
	}
	if !errors.Is(err, fetchErr) {
		t.Fatalf("combined error does not wrap fetchErr: %v", err)
	}
	if !errors.Is(err, workerErr) {
		t.Fatalf("combined error does not wrap workerErr: %v", err)
	}
}

func TestConsumerReturnsCommitErrorAndStopsIntake(t *testing.T) {
	commitErr := errors.New("commit boom")
	reader := &fakeMessageReader{
		messages:  []segmentio.Message{{Partition: 0, Offset: 0, Key: []byte("key-0"), Value: []byte("value-0")}},
		commitErr: commitErr,
	}
	consumer := newConsumer(
		Config{WorkerCount: 1, QueueCapacity: 1, DrainTimeout: 100 * time.Millisecond, QuiescenceWindow: 10 * time.Millisecond},
		reader,
		func() MessageHandler { return successfulHandler{} },
		NewRetryPolicy(1, 10*time.Millisecond, 10*time.Millisecond),
	)

	err := consumer.ConsumeMessages(context.Background())
	if err == nil || !errors.Is(err, commitErr) {
		t.Fatalf("ConsumeMessages() error = %v, want commit error %v", err, commitErr)
	}
	if got := consumer.ProcessedCount(); got != 0 {
		t.Fatalf("ProcessedCount() = %d, want 0", got)
	}
	if got := reader.committedOffsets(); len(got) != 0 {
		t.Fatalf("committed offsets = %v, want none", got)
	}
}
