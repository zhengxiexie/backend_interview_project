package kafka

import "time"

// RetryPolicy defines the bounded retry contract for one processing attempt
// sequence. It is intentionally deterministic so tests and scenario logs can
// reason about exact retry timing.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of *retries* after the first failure.
	// A value of N means the message is processed at most N+1 times in total:
	// one original attempt and up to N retries. Zero means no retries.
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// RetryDecision is the result of asking whether the next retry is still allowed
// and, if so, how long to wait before trying again.
type RetryDecision struct {
	// Retry is true when the caller should attempt the operation again.
	Retry bool
	// Delay is how long to wait before the next attempt. Zero when Retry is false.
	Delay time.Duration
}

// NewRetryPolicy constructs a RetryPolicy with normalized inputs. Negative
// values are clamped to zero; maxDelay is raised to baseDelay if smaller.
func NewRetryPolicy(maxAttempts int, baseDelay, maxDelay time.Duration) RetryPolicy {
	if maxAttempts < 0 {
		maxAttempts = 0
	}
	if baseDelay < 0 {
		baseDelay = 0
	}
	if maxDelay < 0 {
		maxDelay = 0
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	return RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   baseDelay,
		MaxDelay:    maxDelay,
	}
}

func (p RetryPolicy) Decide(attempt int) RetryDecision {
	if attempt < 1 || attempt > p.MaxAttempts {
		return RetryDecision{}
	}

	return RetryDecision{
		Retry: true,
		Delay: p.delayForAttempt(attempt),
	}
}

func (p RetryPolicy) delayForAttempt(attempt int) time.Duration {
	delay := p.BaseDelay
	if delay <= 0 {
		return 0
	}

	for i := 1; i < attempt; i++ {
		if p.MaxDelay > 0 && delay >= p.MaxDelay {
			return p.MaxDelay
		}

		next := delay * 2
		if next < delay {
			if p.MaxDelay > 0 {
				return p.MaxDelay
			}
			return delay
		}

		delay = next
		if p.MaxDelay > 0 && delay > p.MaxDelay {
			return p.MaxDelay
		}
	}

	return delay
}
