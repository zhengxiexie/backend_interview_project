package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// MessageHandler contains the business-side processing contract. The rest of
// the pipeline treats handler success as a local fact, then decides commit
// safety separately.
type MessageHandler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

// HandlerFactory creates a fresh handler for each partitionState so tests and
// scenarios can isolate stateful processing behavior when needed.
type HandlerFactory func() MessageHandler

func NewHandlerFactory(cfg Config) HandlerFactory {
	return func() MessageHandler {
		return NewSimulatedMessageHandler(cfg)
	}
}

// SimulatedMessageHandler is the default demo handler. It intentionally carries
// delay and failure injection hooks so unit tests and e2e scenarios can prove
// retry, replay, and out-of-order commit behavior without adding real business
// side effects.
type SimulatedMessageHandler struct {
	processingDelay         time.Duration
	perKeyProcessingDelays  map[string]time.Duration
	failureMu               sync.Mutex
	simulatedFailureKeys    map[string]int
	simulatedFailureAttempt int
}

func NewSimulatedMessageHandler(cfg Config) MessageHandler {
	simulatedFailureKeys := make(map[string]int, len(cfg.Simulation.FailureKeys))
	for _, key := range cfg.Simulation.FailureKeys {
		simulatedFailureKeys[key] = 0
	}

	perKeyProcessingDelays := make(map[string]time.Duration, len(cfg.PerKeyProcessingDelays))
	for key, delay := range cfg.PerKeyProcessingDelays {
		perKeyProcessingDelays[key] = delay
	}

	return &SimulatedMessageHandler{
		processingDelay:         cfg.ProcessingDelay,
		perKeyProcessingDelays:  perKeyProcessingDelays,
		simulatedFailureKeys:    simulatedFailureKeys,
		simulatedFailureAttempt: cfg.Simulation.FailureAttempts,
	}
}

func (h *SimulatedMessageHandler) Handle(ctx context.Context, msg kafka.Message) error {
	key := string(msg.Key)
	if err := sleepContext(ctx, h.processingDelayForKey(key)); err != nil {
		return err
	}

	if h.simulatedFailureAttempt > 0 {
		h.failureMu.Lock()
		attempts, ok := h.simulatedFailureKeys[key]
		if ok && attempts < h.simulatedFailureAttempt {
			h.simulatedFailureKeys[key] = attempts + 1
			h.failureMu.Unlock()
			return fmt.Errorf("simulated transient failure for key=%s attempt=%d", key, attempts+1)
		}
		h.failureMu.Unlock()
	}

	log.Printf("✓ Processed partition %d offset %d: key=%s, value=%s",
		msg.Partition,
		msg.Offset,
		string(msg.Key),
		string(msg.Value),
	)
	return nil
}

func (h *SimulatedMessageHandler) processingDelayForKey(key string) time.Duration {
	if delay, ok := h.perKeyProcessingDelays[key]; ok {
		return delay
	}
	return h.processingDelay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
