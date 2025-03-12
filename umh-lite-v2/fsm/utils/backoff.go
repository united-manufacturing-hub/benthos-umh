package utils

import (
	"time"
)

// TransitionBackoffManager manages exponential backoff for failed transitions.
type TransitionBackoffManager struct {
	retryCount            int       // Must be at least 1
	lastTransitionAttempt time.Time // The time of the last transition attempt
}

// NewTransitionBackoffManager initializes a new manager with a retryCount of 1.
func NewTransitionBackoffManager() *TransitionBackoffManager {
	return &TransitionBackoffManager{
		retryCount:            1,
		lastTransitionAttempt: time.Now().Add(-2 * time.Minute), // Set to past time to allow first attempt
	}
}

// CalculateBackoffDuration returns the backoff duration based on the current retry count.
// It uses exponential backoff (1s, 2s, 4s, …) capped at 1 minute.
func (b *TransitionBackoffManager) CalculateBackoffDuration() time.Duration {
	// Ensure retryCount is at least 1.
	if b.retryCount < 1 {
		b.retryCount = 1
	}
	backoff := time.Duration(1<<uint(b.retryCount-1)) * time.Second
	if backoff > 1*time.Minute {
		return 1 * time.Minute
	}
	return backoff
}

// ReconcileBackoffElapsed returns true if either there is no error or if the elapsed time
// since the last transition is greater than or equal to the calculated backoff duration.
func (b *TransitionBackoffManager) ReconcileBackoffElapsed() bool {
	backoffDuration := b.CalculateBackoffDuration()
	return time.Since(b.lastTransitionAttempt) >= backoffDuration
}

// IncrementRetryCount increments the retry count and records the current time as the last transition time.
func (b *TransitionBackoffManager) IncrementRetryCount() {
	b.retryCount++
	b.lastTransitionAttempt = time.Now()
}

// Reset clears the error and resets the retry count to 1.
func (b *TransitionBackoffManager) Reset() {
	b.retryCount = 1
	b.lastTransitionAttempt = time.Now()
}
