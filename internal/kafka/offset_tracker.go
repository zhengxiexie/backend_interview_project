package kafka

import (
	"fmt"
	"slices"
	"sync"
)

// OffsetTracker is the correctness core for same-partition concurrency.
//
// Messages may finish out of order, but commits are only safe for the highest
// contiguous completed range starting at lastCommitted + 1.
type OffsetTracker struct {
	mu            sync.Mutex
	lastCommitted int64
	inFlight      map[int64]struct{}
	completed     map[int64]struct{}
}

// TrackerSnapshot is a read-only diagnostic view used by tests to assert the
// frontier state after specific completion orders.
type TrackerSnapshot struct {
	LastCommitted int64
	PendingCount  int
	InFlight      []int64
	Completed     []int64
}

// NewOffsetTracker creates a tracker whose committed frontier starts at
// lastCommitted. Pass msg.Offset-1 for the first message on a partition so
// that the first successful completion immediately advances the frontier.
func NewOffsetTracker(lastCommitted int64) *OffsetTracker {
	return &OffsetTracker{
		lastCommitted: lastCommitted,
		inFlight:      make(map[int64]struct{}),
		completed:     make(map[int64]struct{}),
	}
}

// MarkInFlight registers offset as in-flight before its processing job is
// dispatched to a worker. It must be called for every fetched offset before
// MarkDone is called for that offset.
func (t *OffsetTracker) MarkInFlight(offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset <= t.lastCommitted {
		return
	}

	if _, ok := t.completed[offset]; ok {
		return
	}

	t.inFlight[offset] = struct{}{}
}

// MarkDone records that processing of offset has completed.
// It returns an error if the offset was never marked in-flight, because letting
// an untracked offset advance the frontier would hide a correctness bug in the
// caller.
func (t *OffsetTracker) MarkDone(offset int64) (advanced bool, commitThrough int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if offset <= t.lastCommitted {
		// Anything at or behind the committed frontier is already durable from the
		// consumer's perspective and should not influence a future commit.
		delete(t.inFlight, offset)
		delete(t.completed, offset)
		return false, 0, nil
	}

	if _, ok := t.inFlight[offset]; !ok {
		if _, alreadyCompleted := t.completed[offset]; alreadyCompleted {
			return false, 0, fmt.Errorf("offset %d marked done more than once", offset)
		}
		return false, 0, fmt.Errorf("offset %d marked done before MarkInFlight", offset)
	}

	delete(t.inFlight, offset)
	t.completed[offset] = struct{}{}

	previous := t.lastCommitted
	for next := t.lastCommitted + 1; ; next++ {
		// The frontier only advances while there are no holes. This is the key
		// rule that prevents later successful offsets from skipping unfinished work.
		if _, ok := t.completed[next]; !ok {
			break
		}

		delete(t.completed, next)
		t.lastCommitted = next
	}

	if t.lastCommitted == previous {
		return false, 0, nil
	}

	return true, t.lastCommitted, nil
}

func (t *OffsetTracker) PendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.inFlight) + len(t.completed)
}

func (t *OffsetTracker) Snapshot() TrackerSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()

	inFlight := sortedOffsets(t.inFlight)
	completed := sortedOffsets(t.completed)

	return TrackerSnapshot{
		LastCommitted: t.lastCommitted,
		PendingCount:  len(inFlight) + len(completed),
		InFlight:      inFlight,
		Completed:     completed,
	}
}

func sortedOffsets(set map[int64]struct{}) []int64 {
	offsets := make([]int64, 0, len(set))
	for offset := range set {
		offsets = append(offsets, offset)
	}

	slices.Sort(offsets)

	return offsets
}
