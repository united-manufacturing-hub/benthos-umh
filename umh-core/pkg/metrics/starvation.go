package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/config"
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"
)

// StarvationChecker is a manager that checks if the control loop has been starved
type StarvationChecker struct {
	starvationThreshold time.Duration
	lastReconcileTime   time.Time
	mutex               sync.RWMutex
	logger              *zap.SugaredLogger
}

// NewStarvationChecker creates a new starvation checker
func NewStarvationChecker(threshold time.Duration) *StarvationChecker {
	return &StarvationChecker{
		starvationThreshold: threshold,
		lastReconcileTime:   time.Now(),
		logger:              logger.For(logger.ComponentStarvationChecker),
	}
}

// UpdateLastReconcileTime updates the timestamp of the last reconciliation
func (s *StarvationChecker) UpdateLastReconcileTime() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastReconcileTime = time.Now()
}

// GetLastReconcileTime returns the timestamp of the last reconciliation
func (s *StarvationChecker) GetLastReconcileTime() time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastReconcileTime
}

// GetManagerName returns the name of the manager
func (s *StarvationChecker) GetManagerName() string {
	return logger.ComponentStarvationChecker
}

// Reconcile checks if the control loop has been starved
// Returns error (always nil in this implementation), and whether a starvation was detected
func (s *StarvationChecker) Reconcile(ctx context.Context, config config.FullConfig) (error, bool) {
	s.mutex.RLock()
	timeSinceLastReconcile := time.Since(s.lastReconcileTime)
	s.mutex.RUnlock()

	// Check if we've exceeded the starvation threshold
	if timeSinceLastReconcile > s.starvationThreshold {
		starvationTime := timeSinceLastReconcile.Seconds()
		AddStarvationTime(starvationTime)
		s.logger.Warnf("Control loop starvation detected: %.2f seconds since last reconcile", starvationTime)

		// Update the last reconcile time
		s.UpdateLastReconcileTime()
		return nil, true
	}

	// No starvation detected
	return nil, false
}
