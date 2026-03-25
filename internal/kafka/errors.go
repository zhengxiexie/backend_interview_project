package kafka

import (
	"context"
	"errors"
)

// IgnoreContextCancellation returns nil when err is nil or wraps
// context.Canceled. Any other error — including context.DeadlineExceeded from
// internal timeouts such as CommitTimeout — is returned unchanged so that real
// failures are always surfaced to the caller.
//
// This is used throughout the pipeline to distinguish intentional shutdown
// signals (context cancellation) from genuine processing or commit failures.
func IgnoreContextCancellation(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
