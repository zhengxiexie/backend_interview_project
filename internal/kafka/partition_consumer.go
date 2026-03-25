package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// messageJob is the unit transferred from fetchLoop to workerLoop. It stays
// deliberately small because all correctness state lives outside the queue.
type messageJob struct {
	msg kafka.Message
}

// partitionState holds the correctness-critical state for one partition.
//
// Even though intake and execution are shared across partitions, commit safety
// is always decided partition by partition. That is why tracker and commit cache
// live here instead of on the Consumer itself.
type partitionState struct {
	partition     int
	tracker       *OffsetTracker
	committer     *CommitManager
	handler       MessageHandler
	stateMu       sync.Mutex  // value: guards tracker/committer state for this partition only
	commitMu      *sync.Mutex // pointer: shared across all partitions to serialize Kafka commits
	commitTimeout time.Duration
}

func newPartitionState(cfg Config, partition int, lastCommitted int64, commitMu *sync.Mutex, handler MessageHandler) *partitionState {
	if handler == nil {
		handler = noopMessageHandler{}
	}

	commitTimeout := cfg.CommitTimeout
	if commitTimeout <= 0 {
		commitTimeout = 5 * time.Second
	}

	return &partitionState{
		partition:     partition,
		tracker:       NewOffsetTracker(lastCommitted),
		committer:     NewCommitManager(cfg.Simulation.CommitFailureCount),
		commitMu:      commitMu,
		handler:       handler,
		commitTimeout: commitTimeout,
	}
}

func (ps *partitionState) trackFetched(msg kafka.Message) {
	ps.stateMu.Lock()
	defer ps.stateMu.Unlock()

	// Track and cache the fetched message before execution starts. This guarantees
	// that a later frontier advance can still locate the exact Kafka message used
	// for CommitMessages.
	ps.tracker.MarkInFlight(msg.Offset)
	ps.committer.Track(msg)
}

func (ps *partitionState) handle(ctx context.Context, msg kafka.Message) error {
	return ps.handler.Handle(ctx, msg)
}

func (ps *partitionState) commitProcessed(committer messageCommitter, msg kafka.Message) error {
	ps.stateMu.Lock()
	defer ps.stateMu.Unlock()

	advanced, commitThrough, err := ps.tracker.MarkDone(msg.Offset)
	if err != nil {
		return fmt.Errorf("mark offset %d done for partition %d: %w", msg.Offset, ps.partition, err)
	}
	if !advanced {
		// Later offsets may already be done, but commit must wait until the
		// partition frontier becomes contiguous.
		return nil
	}

	// context.Background() is used intentionally: commit must complete even
	// when the worker context is cancelled during graceful shutdown. Abandoning
	// a successful processing result at the commit step would violate
	// at-least-once semantics by leaving the offset uncommitted.
	commitCtx, cancel := context.WithTimeout(context.Background(), ps.commitTimeout)
	defer cancel()

	ps.commitMu.Lock()
	defer ps.commitMu.Unlock()

	if err := ps.committer.CommitThrough(commitCtx, committer, commitThrough); err != nil {
		return fmt.Errorf("commit through offset %d for partition %d: %w", commitThrough, ps.partition, err)
	}

	log.Printf("✓ Committed partition %d through offset %d", ps.partition, commitThrough)
	return nil
}

// noopMessageHandler exists only as a defensive fallback in tests; production
// code always injects a real handler.
type noopMessageHandler struct{}

func (noopMessageHandler) Handle(context.Context, kafka.Message) error {
	return nil
}
