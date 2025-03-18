package backoff

import (
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"go.uber.org/zap"
)

// Error message constants
const (
	// TemporaryBackoffError indicates a temporary failure with backoff in progress
	TemporaryBackoffError = "operation suspended due to temporary error"

	// PermanentFailureError indicates that max retries were reached
	PermanentFailureError = "operation permanently failed after max retries"
)

// BackoffManager handles error backoff with exponential retries and permanent failure detection
type BackoffManager struct {
	// Mutex for thread safety
	mu sync.RWMutex

	// The last error that occurred
	lastError error

	// The backoff policy
	backoff backoff.BackOff

	// The time when operations can be resumed
	suspendedUntilTime time.Time

	// Flag indicating permanent failure state (max retries exceeded)
	permanentFailure bool

	// Component name for logging
	componentName string

	// Logger
	logger *zap.SugaredLogger
}

// Config holds configuration for creating a new BackoffManager
type Config struct {
	// Initial backoff interval
	InitialInterval time.Duration

	// Maximum backoff interval
	MaxInterval time.Duration

	// Maximum number of retries before permanent failure
	MaxRetries uint64

	// Component name for logging
	ComponentName string

	// Logger
	Logger *zap.SugaredLogger
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(componentName string, logger *zap.SugaredLogger) Config {
	return Config{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Minute,
		MaxRetries:      5,
		ComponentName:   componentName,
		Logger:          logger,
	}
}

// NewBackoffManager creates a new BackoffManager with the given config
func NewBackoffManager(config Config) *BackoffManager {
	// Create exponential backoff with the provided settings
	baseBackoff := backoff.NewExponentialBackOff()
	baseBackoff.InitialInterval = config.InitialInterval
	baseBackoff.MaxInterval = config.MaxInterval

	// Wrap with max retries - after MaxRetries failures, it will return backoff.Stop
	backoffWithMaxRetries := backoff.WithMaxRetries(baseBackoff, config.MaxRetries)

	return &BackoffManager{
		backoff:          backoffWithMaxRetries,
		componentName:    config.ComponentName,
		logger:           config.Logger,
		permanentFailure: false,
	}
}

// SetError records an error and updates the backoff state
// Returns true if the backoff has reached permanent failure state
func (m *BackoffManager) SetError(err error) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastError = err

	// If already permanently failed, don't change the state
	if m.permanentFailure {
		return true
	}

	// Get the next backoff duration
	next := m.backoff.NextBackOff()

	// Check if we've reached permanent failure (backoff.Stop)
	if next == backoff.Stop {
		m.logger.Errorf("%s has exceeded maximum retries, marking as permanently failed", m.componentName)
		m.permanentFailure = true
		m.suspendedUntilTime = time.Time{} // Clear suspension time
		return true
	}

	// Set the suspension time unconditionally for a new backoff period
	m.suspendedUntilTime = time.Now().Add(next)
	m.logger.Debugf("Suspending %s operations for %s because of error: %s",
		m.componentName, next, err)

	return false
}

// Reset clears all error and backoff state
func (m *BackoffManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastError = nil
	m.backoff.Reset()
	m.suspendedUntilTime = time.Time{}
	m.permanentFailure = false
}

// ShouldSkipOperation returns true if operations should be skipped due to backoff
func (m *BackoffManager) ShouldSkipOperation() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// If permanently failed, operations should always be skipped
	if m.permanentFailure {
		return true
	}

	// If there is no error or no suspension time, don't skip
	if m.lastError == nil || m.suspendedUntilTime.IsZero() {
		return false
	}

	// If the backoff period has not yet elapsed, skip the operation
	if time.Now().Before(m.suspendedUntilTime) {
		m.logger.Debugf("Skipping %s operation because of error: %s. Remaining backoff: %s",
			m.componentName, m.lastError, time.Until(m.suspendedUntilTime))
		return true
	}

	// Backoff period has elapsed, we can proceed with the operation
	// Note: We don't reset the suspendedUntilTime here to avoid race conditions
	// It will be explicitly reset on successful operations via Reset()
	// or updated on new errors via SetError()
	return false
}

// IsPermanentlyFailed returns true if the max retry count has been exceeded
func (m *BackoffManager) IsPermanentlyFailed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.permanentFailure
}

// GetLastError returns the last error recorded
func (m *BackoffManager) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

// GetBackoffError returns an appropriate error message based on the current state:
// - For permanent failures, it returns a permanent failure error
// - For temporary backoffs, it returns a temporary backoff error with retry time
// - If no backoff is in progress, it returns nil
func (m *BackoffManager) GetBackoffError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.permanentFailure {
		return fmt.Errorf("%s: %w", PermanentFailureError, m.lastError)
	}

	if m.lastError != nil && !m.suspendedUntilTime.IsZero() && time.Now().Before(m.suspendedUntilTime) {
		retryAfter := time.Until(m.suspendedUntilTime)
		return fmt.Errorf("%s (retry after %v): %w", TemporaryBackoffError, retryAfter, m.lastError)
	}

	return nil
}

// SetErrorWithBackoffForTesting is a test-only helper that allows injection of a custom backoff policy
// This is used to create more deterministic tests
func (m *BackoffManager) SetErrorWithBackoffForTesting(err error, customBackoff backoff.BackOff) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastError = err
	m.backoff = customBackoff

	// Set up the backoff period
	next := m.backoff.NextBackOff()
	if next == backoff.Stop {
		m.permanentFailure = true
		return true
	}

	m.suspendedUntilTime = time.Now().Add(next)
	return false
}
