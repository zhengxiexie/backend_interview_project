package kafka

import (
	"container/heap"
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type messageCommitter interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type offsetEntry struct {
	offset int64
	msg    kafka.Message
}

type offsetHeap []offsetEntry

func (h *offsetHeap) Len() int           { return len(*h) }
func (h *offsetHeap) Less(i, j int) bool { return (*h)[i].offset < (*h)[j].offset }
func (h *offsetHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *offsetHeap) Push(x any)         { *h = append(*h, x.(offsetEntry)) }
func (h *offsetHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// CommitManager caches in-flight Kafka messages by offset and provides
// CommitThrough to commit and evict all messages up to a given frontier offset.
//
// Thread safety: Track and CommitThrough are individually goroutine-safe via
// an internal mutex, but the caller MUST serialize CommitThrough calls for the
// same partition externally. Without external serialization, two concurrent
// CommitThrough calls for different frontier offsets may interleave their
// commit-then-evict phases, leading to cache inconsistency. In this project,
// partitionState.commitMu provides that guarantee.
//
// simulatedFailureCount is a test-only hook that causes the next N
// CommitThrough calls to fail before touching the broker.
type CommitManager struct {
	mu                    sync.Mutex
	msgMap                map[int64]kafka.Message
	heap                  offsetHeap
	simulatedFailureCount int
}

func NewCommitManager(simulatedFailureCount int) *CommitManager {
	return &CommitManager{
		msgMap:                make(map[int64]kafka.Message),
		simulatedFailureCount: simulatedFailureCount,
	}
}

func (m *CommitManager) Track(msg kafka.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.msgMap[msg.Offset]; !exists {
		heap.Push(&m.heap, offsetEntry{offset: msg.Offset, msg: msg})
	}
	m.msgMap[msg.Offset] = msg
}

func (m *CommitManager) CommitThrough(ctx context.Context, committer messageCommitter, offset int64) error {
	m.mu.Lock()

	item, ok := m.msgMap[offset]
	if !ok {
		// Release the lock before returning so we never consume a fault-budget slot
		// for a message that was never tracked.
		m.mu.Unlock()
		return fmt.Errorf("commit message for offset %d not found", offset)
	}
	failureCount := m.simulatedFailureCount
	if failureCount > 0 {
		m.simulatedFailureCount--
	}
	m.mu.Unlock()

	if failureCount > 0 {
		return fmt.Errorf("simulated commit failure for offset %d", offset)
	}

	if err := committer.CommitMessages(ctx, item); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for m.heap.Len() > 0 && m.heap[0].offset <= offset {
		entry := heap.Pop(&m.heap).(offsetEntry)
		delete(m.msgMap, entry.offset)
	}

	return nil
}
