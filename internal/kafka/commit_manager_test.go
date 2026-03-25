package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	segmentio "github.com/segmentio/kafka-go"
)

// fakeCommitter records which messages were committed.
type fakeCommitter struct {
	committed []segmentio.Message
	err       error
}

func (f *fakeCommitter) CommitMessages(_ context.Context, msgs ...segmentio.Message) error {
	if f.err != nil {
		return f.err
	}
	f.committed = append(f.committed, msgs...)
	return nil
}

func TestCommitManagerCommitThroughSucceeds(t *testing.T) {
	m := NewCommitManager(0)
	msg0 := segmentio.Message{Offset: 0}
	msg1 := segmentio.Message{Offset: 1}
	msg2 := segmentio.Message{Offset: 2}
	m.Track(msg0)
	m.Track(msg1)
	m.Track(msg2)

	committer := &fakeCommitter{}
	if err := m.CommitThrough(context.Background(), committer, 2); err != nil {
		t.Fatalf("CommitThrough() error = %v, want nil", err)
	}

	if len(committer.committed) != 1 || committer.committed[0].Offset != 2 {
		t.Fatalf("committed messages = %v, want [{Offset:2}]", committer.committed)
	}
}

func TestCommitManagerCleansUpCachedMessagesAfterCommit(t *testing.T) {
	m := NewCommitManager(0)
	m.Track(segmentio.Message{Offset: 0})
	m.Track(segmentio.Message{Offset: 1})
	m.Track(segmentio.Message{Offset: 2})

	committer := &fakeCommitter{}
	if err := m.CommitThrough(context.Background(), committer, 1); err != nil {
		t.Fatalf("CommitThrough() error = %v, want nil", err)
	}

	// Messages at offset <= 1 should be evicted; offset 2 must still be tracked.
	// Committing offset 2 again should succeed.
	if err := m.CommitThrough(context.Background(), committer, 2); err != nil {
		t.Fatalf("second CommitThrough() error = %v, want nil", err)
	}
	if len(committer.committed) != 2 {
		t.Fatalf("expected 2 total commits, got %d", len(committer.committed))
	}
}

func TestCommitManagerSimulatedFailureReturnsError(t *testing.T) {
	m := NewCommitManager(1) // one simulated failure
	m.Track(segmentio.Message{Offset: 0})

	committer := &fakeCommitter{}
	err := m.CommitThrough(context.Background(), committer, 0)
	if err == nil {
		t.Fatal("CommitThrough() expected simulated failure error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated commit failure") {
		t.Fatalf("CommitThrough() error = %q, want message containing \"simulated commit failure\"", err)
	}
	if len(committer.committed) != 0 {
		t.Fatalf("expected no real commits after simulated failure, got %v", committer.committed)
	}
}

func TestCommitManagerSimulatedFailureOnlyOccursOnce(t *testing.T) {
	m := NewCommitManager(1) // exactly one failure
	m.Track(segmentio.Message{Offset: 0})
	m.Track(segmentio.Message{Offset: 1})

	committer := &fakeCommitter{}

	// First commit: simulated failure
	if err := m.CommitThrough(context.Background(), committer, 0); err == nil {
		t.Fatal("expected first CommitThrough to fail")
	}

	// Second commit: should succeed
	if err := m.CommitThrough(context.Background(), committer, 1); err != nil {
		t.Fatalf("second CommitThrough() error = %v, want nil", err)
	}
}

func TestCommitManagerUnknownOffsetReturnsError(t *testing.T) {
	m := NewCommitManager(0)
	// No Track() call — offset 99 is unknown.
	err := m.CommitThrough(context.Background(), &fakeCommitter{}, 99)
	if err == nil {
		t.Fatal("CommitThrough() with unknown offset should return error, got nil")
	}
}

func TestCommitManagerUnknownOffsetDoesNotConsumeFaultBudget(t *testing.T) {
	// An unknown-offset call must not burn a fault-budget slot. If it did, a
	// subsequent real commit would silently succeed instead of triggering the
	// configured simulated failure.
	m := NewCommitManager(1) // one fault in budget

	// Call with an untracked offset — error expected, fault budget must be intact.
	if err := m.CommitThrough(context.Background(), &fakeCommitter{}, 99); err == nil {
		t.Fatal("CommitThrough() with unknown offset should return error")
	}

	// Now track a real message and commit it: the fault budget must still trigger.
	m.Track(segmentio.Message{Offset: 0})
	committer := &fakeCommitter{}
	if err := m.CommitThrough(context.Background(), committer, 0); err == nil {
		t.Fatal("expected simulated failure on first real commit — fault budget was consumed by unknown-offset call")
	}
	if len(committer.committed) != 0 {
		t.Fatalf("expected no real commits after simulated failure, got %v", committer.committed)
	}
}

func TestCommitManagerPropagatesCommitterError(t *testing.T) {
	m := NewCommitManager(0)
	m.Track(segmentio.Message{Offset: 0})

	committer := &fakeCommitter{err: errors.New("broker unavailable")}
	err := m.CommitThrough(context.Background(), committer, 0)
	if err == nil || !errors.Is(err, committer.err) {
		t.Fatalf("CommitThrough() error = %v, want %v", err, committer.err)
	}
}
