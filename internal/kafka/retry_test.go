package kafka

import (
	"testing"
	"time"
)

func TestRetryPolicyZeroMaxAttempts(t *testing.T) {
	policy := NewRetryPolicy(0, 100*time.Millisecond, time.Second)

	// Any attempt should return no-retry when MaxAttempts is zero.
	for _, attempt := range []int{0, 1, 2, 99} {
		got := policy.Decide(attempt)
		if got.Retry || got.Delay != 0 {
			t.Fatalf("Decide(%d) with MaxAttempts=0 = %+v, want {Retry:false, Delay:0}", attempt, got)
		}
	}
}

func TestNewRetryPolicyNormalizesNegativeInputs(t *testing.T) {
	// Negative values must be clamped to zero, not left as-is.
	policy := NewRetryPolicy(-5, -100*time.Millisecond, -1*time.Second)

	if policy.MaxAttempts != 0 {
		t.Fatalf("MaxAttempts = %d, want 0", policy.MaxAttempts)
	}
	if policy.BaseDelay != 0 {
		t.Fatalf("BaseDelay = %v, want 0", policy.BaseDelay)
	}
	if policy.MaxDelay != 0 {
		t.Fatalf("MaxDelay = %v, want 0", policy.MaxDelay)
	}
	// Decide should always return no-retry for a zero policy.
	got := policy.Decide(1)
	if got.Retry {
		t.Fatalf("Decide(1) on zero policy = %+v, want no-retry", got)
	}
}

func TestRetryPolicy(t *testing.T) {
	t.Run("retry stops after max attempts", func(t *testing.T) {
		policy := NewRetryPolicy(3, 100*time.Millisecond, time.Second)

		tests := []struct {
			name    string
			attempt int
			want    RetryDecision
		}{
			{
				name:    "attempt zero does not retry",
				attempt: 0,
				want:    RetryDecision{},
			},
			{
				name:    "first attempt retries",
				attempt: 1,
				want: RetryDecision{
					Retry: true,
					Delay: 100 * time.Millisecond,
				},
			},
			{
				name:    "max attempt still retries",
				attempt: 3,
				want: RetryDecision{
					Retry: true,
					Delay: 400 * time.Millisecond,
				},
			},
			{
				name:    "attempt after max does not retry",
				attempt: 4,
				want:    RetryDecision{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := policy.Decide(tt.attempt)
				if got != tt.want {
					t.Fatalf("Decide(%d) = %+v, want %+v", tt.attempt, got, tt.want)
				}
			})
		}
	})

	t.Run("delay grows exponentially", func(t *testing.T) {
		policy := NewRetryPolicy(4, 50*time.Millisecond, time.Second)

		wantDelays := []time.Duration{
			50 * time.Millisecond,
			100 * time.Millisecond,
			200 * time.Millisecond,
			400 * time.Millisecond,
		}

		for i, wantDelay := range wantDelays {
			attempt := i + 1
			got := policy.Decide(attempt)
			if !got.Retry {
				t.Fatalf("Decide(%d) = %+v, want retry enabled", attempt, got)
			}
			if got.Delay != wantDelay {
				t.Fatalf("Decide(%d) delay = %v, want %v", attempt, got.Delay, wantDelay)
			}
		}
	})

	t.Run("delay is capped at max delay", func(t *testing.T) {
		policy := NewRetryPolicy(5, 150*time.Millisecond, 500*time.Millisecond)

		wantDelays := []time.Duration{
			150 * time.Millisecond,
			300 * time.Millisecond,
			500 * time.Millisecond,
			500 * time.Millisecond,
			500 * time.Millisecond,
		}

		for i, wantDelay := range wantDelays {
			attempt := i + 1
			got := policy.Decide(attempt)
			if !got.Retry {
				t.Fatalf("Decide(%d) = %+v, want retry enabled", attempt, got)
			}
			if got.Delay != wantDelay {
				t.Fatalf("Decide(%d) delay = %v, want %v", attempt, got.Delay, wantDelay)
			}
		}
	})
}
