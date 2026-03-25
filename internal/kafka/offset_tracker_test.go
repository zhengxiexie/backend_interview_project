package kafka

import (
	"reflect"
	"sync"
	"testing"
)

func TestOffsetTrackerSequentialCompletionAdvancesFrontierNormally(t *testing.T) {
	tracker := NewOffsetTracker(100)
	markInFlight(tracker, 101, 102, 103)

	assertPendingCount(t, tracker, 3)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 100,
		PendingCount:  3,
		InFlight:      []int64{101, 102, 103},
		Completed:     []int64{},
	})

	advanced, commitThrough, err := tracker.MarkDone(101)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, true, 101)
	assertPendingCount(t, tracker, 2)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 101,
		PendingCount:  2,
		InFlight:      []int64{102, 103},
		Completed:     []int64{},
	})

	advanced, commitThrough, err = tracker.MarkDone(102)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, true, 102)
	assertPendingCount(t, tracker, 1)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 102,
		PendingCount:  1,
		InFlight:      []int64{103},
		Completed:     []int64{},
	})

	advanced, commitThrough, err = tracker.MarkDone(103)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, true, 103)
	assertPendingCount(t, tracker, 0)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 103,
		PendingCount:  0,
		InFlight:      []int64{},
		Completed:     []int64{},
	})
}

func TestOffsetTrackerOutOfOrderCompletionDoesNotSkipUnfinishedOffsets(t *testing.T) {
	tracker := NewOffsetTracker(200)
	markInFlight(tracker, 201, 202, 203)

	advanced, commitThrough, err := tracker.MarkDone(202)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, false, 0)
	assertPendingCount(t, tracker, 3)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 200,
		PendingCount:  3,
		InFlight:      []int64{201, 203},
		Completed:     []int64{202},
	})

	advanced, commitThrough, err = tracker.MarkDone(203)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, false, 0)
	assertPendingCount(t, tracker, 3)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 200,
		PendingCount:  3,
		InFlight:      []int64{201},
		Completed:     []int64{202, 203},
	})
}

func TestOffsetTrackerDelayedEarlierCompletionUnlocksLargerFrontierWindow(t *testing.T) {
	tracker := NewOffsetTracker(300)
	markInFlight(tracker, 301, 302, 303, 304)

	for _, offset := range []int64{302, 303, 304} {
		advanced, commitThrough, err := tracker.MarkDone(offset)
		if err != nil {
			t.Fatalf("MarkDone(%d) unexpected error = %v", offset, err)
		}
		assertAdvance(t, advanced, commitThrough, false, 0)
	}

	assertPendingCount(t, tracker, 4)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 300,
		PendingCount:  4,
		InFlight:      []int64{301},
		Completed:     []int64{302, 303, 304},
	})

	advanced, commitThrough, err := tracker.MarkDone(301)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, true, 304)
	assertPendingCount(t, tracker, 0)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 304,
		PendingCount:  0,
		InFlight:      []int64{},
		Completed:     []int64{},
	})
}

func TestOffsetTrackerPendingOffsetsRemainUncommitted(t *testing.T) {
	tracker := NewOffsetTracker(400)
	markInFlight(tracker, 401, 402, 403)

	advanced, commitThrough, err := tracker.MarkDone(401)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, true, 401)

	advanced, commitThrough, err = tracker.MarkDone(403)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	assertAdvance(t, advanced, commitThrough, false, 0)
	assertPendingCount(t, tracker, 2)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 401,
		PendingCount:  2,
		InFlight:      []int64{402},
		Completed:     []int64{403},
	})
}

func TestOffsetTrackerMarkDoneOnAlreadyCommittedOffsetIsNoop(t *testing.T) {
	tracker := NewOffsetTracker(100)
	markInFlight(tracker, 101)

	// Mark 101 done normally so lastCommitted advances to 101.
	advanced, _, err := tracker.MarkDone(101)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error = %v", err)
	}
	if !advanced {
		t.Fatal("expected frontier to advance on first MarkDone")
	}

	// Calling MarkDone again on 101 (now <= lastCommitted) must not advance or panic.
	advanced, commitThrough, err := tracker.MarkDone(101)
	if err != nil {
		t.Fatalf("MarkDone() unexpected error on committed offset = %v", err)
	}
	if advanced || commitThrough != 0 {
		t.Fatalf("MarkDone on committed offset = (%t, %d), want (false, 0)", advanced, commitThrough)
	}

	// State must be clean — no stale entries.
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 101,
		PendingCount:  0,
		InFlight:      []int64{},
		Completed:     []int64{},
	})
}

func TestOffsetTrackerMarkDoneWithoutMarkInFlightReturnsError(t *testing.T) {
	tracker := NewOffsetTracker(100)

	advanced, commitThrough, err := tracker.MarkDone(101)
	if err == nil {
		t.Fatal("MarkDone() without MarkInFlight should return error")
	}
	if advanced || commitThrough != 0 {
		t.Fatalf("MarkDone() = (%t, %d), want (false, 0) on misuse", advanced, commitThrough)
	}
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 100,
		PendingCount:  0,
		InFlight:      []int64{},
		Completed:     []int64{},
	})
}

func TestOffsetTrackerDoubleMarkDoneBeforeCommitReturnsError(t *testing.T) {
	tracker := NewOffsetTracker(100)
	markInFlight(tracker, 101, 102)

	advanced, commitThrough, err := tracker.MarkDone(102)
	if err != nil {
		t.Fatalf("first MarkDone() unexpected error = %v", err)
	}
	if advanced || commitThrough != 0 {
		t.Fatalf("first MarkDone() = (%t, %d), want (false, 0)", advanced, commitThrough)
	}

	advanced, commitThrough, err = tracker.MarkDone(102)
	if err == nil {
		t.Fatal("second MarkDone() on same in-flight offset should return error")
	}
	if advanced || commitThrough != 0 {
		t.Fatalf("second MarkDone() = (%t, %d), want (false, 0)", advanced, commitThrough)
	}
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 100,
		PendingCount:  2,
		InFlight:      []int64{101},
		Completed:     []int64{102},
	})
}

func TestOffsetTrackerMarkInFlightSkipsCommittedAndCompletedOffsets(t *testing.T) {
	tracker := NewOffsetTracker(100)
	markInFlight(tracker, 101, 102)

	// 102 completes first, enters the completed map (hole at 101 blocks frontier).
	if _, _, err := tracker.MarkDone(102); err != nil {
		t.Fatalf("MarkDone(102) unexpected error = %v", err)
	}

	// Calling MarkInFlight again on 102 (already in completed) must be a no-op.
	tracker.MarkInFlight(102)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 100,
		PendingCount:  2,
		InFlight:      []int64{101},
		Completed:     []int64{102},
	})

	// 101 completes, frontier advances to 102.
	if _, _, err := tracker.MarkDone(101); err != nil {
		t.Fatalf("MarkDone(101) unexpected error = %v", err)
	}

	// Now both offsets are committed (lastCommitted = 102).
	// MarkInFlight on any committed offset must also be a no-op.
	tracker.MarkInFlight(101)
	tracker.MarkInFlight(102)
	assertSnapshot(t, tracker, TrackerSnapshot{
		LastCommitted: 102,
		PendingCount:  0,
		InFlight:      []int64{},
		Completed:     []int64{},
	})
}

func TestOffsetTrackerConcurrentMarkInFlightAndMarkDone(t *testing.T) {
	// Run with -race to verify no data races under concurrent access.
	const numOffsets = 100
	tracker := NewOffsetTracker(-1)

	// Pre-register all offsets as in-flight.
	for i := int64(0); i < numOffsets; i++ {
		tracker.MarkInFlight(i)
	}

	// Complete all offsets concurrently from multiple goroutines.
	var wg sync.WaitGroup
	for i := int64(0); i < numOffsets; i++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			_, _, _ = tracker.MarkDone(offset)
		}(i)
	}
	wg.Wait()

	// All offsets completed: frontier must be at the last offset.
	snap := tracker.Snapshot()
	if snap.LastCommitted != numOffsets-1 {
		t.Fatalf("LastCommitted = %d, want %d", snap.LastCommitted, numOffsets-1)
	}
	if snap.PendingCount != 0 {
		t.Fatalf("PendingCount = %d, want 0", snap.PendingCount)
	}
}

func markInFlight(tracker *OffsetTracker, offsets ...int64) {
	for _, offset := range offsets {
		tracker.MarkInFlight(offset)
	}
}

func assertAdvance(t *testing.T, gotAdvanced bool, gotCommitThrough int64, wantAdvanced bool, wantCommitThrough int64) {
	t.Helper()

	if gotAdvanced != wantAdvanced || gotCommitThrough != wantCommitThrough {
		t.Fatalf("MarkDone() = (%t, %d), want (%t, %d)", gotAdvanced, gotCommitThrough, wantAdvanced, wantCommitThrough)
	}
}

func assertPendingCount(t *testing.T, tracker *OffsetTracker, want int) {
	t.Helper()

	if got := tracker.PendingCount(); got != want {
		t.Fatalf("PendingCount() = %d, want %d", got, want)
	}
}

func assertSnapshot(t *testing.T, tracker *OffsetTracker, want TrackerSnapshot) {
	t.Helper()

	if got := tracker.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %+v, want %+v", got, want)
	}
}
