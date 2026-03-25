package kafka

import (
	"errors"
	"time"
)

// SimulationConfig carries fault-injection knobs used only by tests and e2e
// scenarios. Keeping them separate from the core Config makes the production
// vs. test boundary explicit.
type SimulationConfig struct {
	FailureKeys        []string
	FailureAttempts    int
	CommitFailureCount int
}

// Config collects all Kafka-side runtime knobs used by the demo app, tests, and
// e2e scenario runner. SimulationConfig is kept as a nested field so simulation
// hooks are never mixed with production settings at a glance.
type Config struct {
	BrokerAddress     string
	ControllerAddress string
	Topic             string
	Partitions        int
	GroupID           string
	WorkerCount       int
	// QueueCapacity is the buffer size of the internal job channel between the
	// fetch loop and the worker pool. Zero creates an unbuffered channel, which
	// limits concurrency to one in-flight message per worker at a time.
	QueueCapacity    int
	MaxRetryAttempts int
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
	// CommitTimeout caps how long a single CommitMessages call may block.
	// Defaults to 5s if zero.
	CommitTimeout time.Duration
	// DrainTimeout is the maximum time the consumer waits for in-flight jobs to
	// complete after intake stops. Zero or negative disables draining entirely.
	DrainTimeout time.Duration
	// QuiescenceWindow is the poll interval used while waiting for drain to
	// complete. Defaults to 100ms if zero.
	QuiescenceWindow time.Duration
	// ProcessingDelay and PerKeyProcessingDelays add artificial latency for
	// scenario demonstration; zero means no added delay.
	ProcessingDelay        time.Duration
	PerKeyProcessingDelays map[string]time.Duration
	// ProducerSendDelay is the pause between successive WriteMessages calls in
	// the demo producer. Defaults to 100ms if zero.
	ProducerSendDelay time.Duration
	Simulation        SimulationConfig
}

// Validate checks Config invariants that must hold before the consumer or
// producer can start safely. Returning an error here prevents silent
// mis-configuration from turning into runtime surprises.
func (c Config) Validate() error {
	var errs []error

	if c.BrokerAddress == "" {
		errs = append(errs, errors.New("BrokerAddress must not be empty"))
	}
	if c.Topic == "" {
		errs = append(errs, errors.New("Topic must not be empty"))
	}
	if c.GroupID == "" {
		errs = append(errs, errors.New("GroupID must not be empty"))
	}
	if c.Partitions < 1 {
		errs = append(errs, errors.New("Partitions must be >= 1"))
	}
	if c.WorkerCount < 1 {
		errs = append(errs, errors.New("WorkerCount must be >= 1"))
	}
	if c.RetryBaseDelay < 0 {
		errs = append(errs, errors.New("RetryBaseDelay must not be negative"))
	}
	if c.RetryMaxDelay < 0 {
		errs = append(errs, errors.New("RetryMaxDelay must not be negative"))
	}
	if c.RetryMaxDelay > 0 && c.RetryBaseDelay > 0 && c.RetryMaxDelay < c.RetryBaseDelay {
		errs = append(errs, errors.New("RetryMaxDelay must be >= RetryBaseDelay"))
	}
	if c.CommitTimeout < 0 {
		errs = append(errs, errors.New("CommitTimeout must not be negative"))
	}
	if c.DrainTimeout < 0 {
		errs = append(errs, errors.New("DrainTimeout must not be negative"))
	}

	return errors.Join(errs...)
}
