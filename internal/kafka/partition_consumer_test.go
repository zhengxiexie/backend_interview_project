package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	segmentio "github.com/segmentio/kafka-go"
)

// newTestPartitionState creates a partitionState starting just before offset 0
// so the first message at offset 0 is always eligible to advance the frontier.
func newTestPartitionState(commitFailures int) *partitionState {
	mu := &sync.Mutex{}
	return newPartitionState(
		Config{Simulation: SimulationConfig{CommitFailureCount: commitFailures}},
		0,  // partition
		-1, // lastCommitted: one before offset 0
		mu,
		successfulHandler{}, // reused from consumer_test.go
	)
}

func TestPartitionStateInOrderCommitAdvancesFrontier(t *testing.T) {
	ps := newTestPartitionState(0)
	msg0 := segmentio.Message{Partition: 0, Offset: 0}
	msg1 := segmentio.Message{Partition: 0, Offset: 1}

	ps.trackFetched(msg0)
	ps.trackFetched(msg1)

	committer := &fakeCommitter{} // reused from commit_manager_test.go

	if err := ps.commitProcessed(committer, msg0); err != nil {
		t.Fatalf("commitProcessed(offset=0) error = %v, want nil", err)
	}
	if len(committer.committed) != 1 || committer.committed[0].Offset != 0 {
		t.Fatalf("after offset 0: committed = %v, want [offset=0]", committer.committed)
	}

	if err := ps.commitProcessed(committer, msg1); err != nil {
		t.Fatalf("commitProcessed(offset=1) error = %v, want nil", err)
	}
	if len(committer.committed) != 2 || committer.committed[1].Offset != 1 {
		t.Fatalf("after offset 1: committed = %v, want [offset=0, offset=1]", committer.committed)
	}
}

func TestPartitionStateOutOfOrderCompletionWaitsForGapThenBatchCommits(t *testing.T) {
	ps := newTestPartitionState(0)
	msg0 := segmentio.Message{Partition: 0, Offset: 0}
	msg1 := segmentio.Message{Partition: 0, Offset: 1}
	msg2 := segmentio.Message{Partition: 0, Offset: 2}

	ps.trackFetched(msg0)
	ps.trackFetched(msg1)
	ps.trackFetched(msg2)

	committer := &fakeCommitter{}

	// offset 1 and 2 finish first — gap at 0 blocks commit.
	if err := ps.commitProcessed(committer, msg1); err != nil {
		t.Fatalf("commitProcessed(offset=1) error = %v", err)
	}
	if err := ps.commitProcessed(committer, msg2); err != nil {
		t.Fatalf("commitProcessed(offset=2) error = %v", err)
	}
	if len(committer.committed) != 0 {
		t.Fatalf("expected no commits while gap at offset 0 exists, got %v", committer.committed)
	}

	// offset 0 finishes — frontier jumps straight to 2 in a single commit.
	if err := ps.commitProcessed(committer, msg0); err != nil {
		t.Fatalf("commitProcessed(offset=0) error = %v", err)
	}
	if len(committer.committed) != 1 || committer.committed[0].Offset != 2 {
		t.Fatalf("expected single commit at offset=2, got %v", committer.committed)
	}
}

func TestPartitionStateCommitFailurePropagatesError(t *testing.T) {
	ps := newTestPartitionState(1) // one simulated commit failure
	msg := segmentio.Message{Partition: 0, Offset: 0}
	ps.trackFetched(msg)

	committer := &fakeCommitter{}
	if err := ps.commitProcessed(committer, msg); err == nil {
		t.Fatal("commitProcessed() expected error from simulated commit failure, got nil")
	}
}

func TestPartitionStateNoCommitWhenFrontierDoesNotAdvance(t *testing.T) {
	ps := newTestPartitionState(0)
	msg0 := segmentio.Message{Partition: 0, Offset: 0}
	msg1 := segmentio.Message{Partition: 0, Offset: 1}

	ps.trackFetched(msg0)
	ps.trackFetched(msg1)

	// offset 1 finishes while offset 0 is still in-flight: frontier must not move.
	committer := &fakeCommitter{}
	if err := ps.commitProcessed(committer, msg1); err != nil {
		t.Fatalf("commitProcessed(offset=1) error = %v, want nil", err)
	}
	if len(committer.committed) != 0 {
		t.Fatalf("unexpected commit while gap at offset 0 exists: %v", committer.committed)
	}
}

// stalledCommitter blocks CommitMessages until ctx is cancelled, simulating
// a broker that hangs during commit.
type stalledCommitter struct {
	started chan struct{}
}

func (s *stalledCommitter) CommitMessages(ctx context.Context, _ ...segmentio.Message) error {
	if s.started != nil {
		close(s.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestPartitionStateCommitTimeoutSurfacesError(t *testing.T) {
	mu := &sync.Mutex{}
	ps := newPartitionState(
		Config{
			CommitTimeout: 50 * time.Millisecond, // short timeout to keep the test fast
		},
		0,  // partition
		-1, // lastCommitted
		mu,
		successfulHandler{},
	)

	msg := segmentio.Message{Partition: 0, Offset: 0}
	ps.trackFetched(msg)

	committer := &stalledCommitter{started: make(chan struct{})}
	err := ps.commitProcessed(committer, msg)
	if err == nil {
		t.Fatal("commitProcessed() expected timeout error when committer stalls, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("commitProcessed() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPartitionStateCommitErrorFromBrokerPropagates(t *testing.T) {
	ps := newTestPartitionState(0)
	msg := segmentio.Message{Partition: 0, Offset: 0}
	ps.trackFetched(msg)

	brokerErr := errors.New("broker unavailable")
	committer := &fakeCommitter{err: brokerErr}
	err := ps.commitProcessed(committer, msg)
	if err == nil || !errors.Is(err, brokerErr) {
		t.Fatalf("commitProcessed() error = %v, want %v", err, brokerErr)
	}
}
