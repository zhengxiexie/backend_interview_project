package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestIgnoreContextCancellationReturnsNilForNil(t *testing.T) {
	if got := IgnoreContextCancellation(nil); got != nil {
		t.Fatalf("IgnoreContextCancellation(nil) = %v, want nil", got)
	}
}

func TestIgnoreContextCancellationReturnsNilForCanceled(t *testing.T) {
	if got := IgnoreContextCancellation(context.Canceled); got != nil {
		t.Fatalf("IgnoreContextCancellation(context.Canceled) = %v, want nil", got)
	}
}

func TestIgnoreContextCancellationReturnsNilForWrappedCanceled(t *testing.T) {
	wrapped := fmt.Errorf("fetch failed: %w", context.Canceled)
	if got := IgnoreContextCancellation(wrapped); got != nil {
		t.Fatalf("IgnoreContextCancellation(wrapped Canceled) = %v, want nil", got)
	}
}

func TestIgnoreContextCancellationPreservesDeadlineExceeded(t *testing.T) {
	if got := IgnoreContextCancellation(context.DeadlineExceeded); got == nil {
		t.Fatal("IgnoreContextCancellation(DeadlineExceeded) = nil, want non-nil")
	}
}

func TestIgnoreContextCancellationPreservesOtherErrors(t *testing.T) {
	other := errors.New("broker unavailable")
	if got := IgnoreContextCancellation(other); got != other {
		t.Fatalf("IgnoreContextCancellation(other) = %v, want %v", got, other)
	}
}
